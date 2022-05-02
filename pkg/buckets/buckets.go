package buckets

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/StevenRojas/gcpstorage/pkg/model"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	fileHandlerCount = 10                // 10000
	maxFileSize      = 1000              // 1_000_000_000
	groupSize        = 128 * 1024 * 1024 //128M
	recordsChBuffer  = 10
)

var (
	parquetRoutines = runtime.NumCPU()
	workersCount    = runtime.NumCPU()
)

var (
	ErrHandlerNotFound = errors.New("the handler for the sample was not found")
)

type parquetHandler struct {
	fw   *storage.Writer
	pw   *writer.ParquetWriter
	size int
}

type BucketHandler struct {
	client       *storage.Client
	bucket       *storage.BucketHandle
	projectID    string
	bucketName   string
	bucketDir    string
	fileHandlers map[int]*parquetHandler
	recordsCh    chan model.AdRecord
	errCh        chan error
	mutex        sync.RWMutex
}

func NewBucketHandler(ctx context.Context, projectID string, bucketName string, bucketDir string) (*BucketHandler, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &BucketHandler{
		client:       client,
		bucket:       client.Bucket(bucketName),
		projectID:    projectID,
		bucketName:   bucketName,
		bucketDir:    bucketDir,
		fileHandlers: make(map[int]*parquetHandler, fileHandlerCount),
	}, nil
}

func (bh *BucketHandler) StartWorkers(ctx context.Context) (chan model.AdRecord, chan error) {
	bh.recordsCh = make(chan model.AdRecord, recordsChBuffer)
	bh.errCh = make(chan error)
	bh.createHandlers(ctx)
	go bh.createWorkers(ctx)

	// create 10k file handlers mapping by sample
	// start writer workers listening into a channel (return the channel)
	// get a parquet message and based on the sample get the file handler
	// write the parquet message into the file handler and increase the file size counter
	// when the file size counter reach a threshold, close the file and create another handler
	// graceful shutdown and panic recovery to close all the handlers
	// schedule a midnight task to pause goroutines, close all handlers, create new ones in new folder and restart goroutines ??

	return bh.recordsCh, bh.errCh
}

func (bh *BucketHandler) Close() {
	bh.closeHandlers()
	close(bh.errCh)
}

func (bh *BucketHandler) createWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go bh.worker(ctx, &wg)
	}
	wg.Wait()
}

func (bh *BucketHandler) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("creating worker", len(bh.recordsCh))
	for rec := range bh.recordsCh {
		log.Println("message", rec)
		select {
		case <-ctx.Done():
			return
		default:
		}
		bh.mutex.Lock()
		ph, ok := bh.fileHandlers[rec.Sample]
		if !ok {
			bh.createHandler(ctx, rec.Sample)
			ph = bh.fileHandlers[rec.Sample]
		}
		if err := ph.pw.Write(rec); err != nil {
			log.Println("Write error", err)
			bh.errCh <- err
		}
		ph.size += rec.Size
		if ph.size > maxFileSize {
			log.Println("Max File Size reach, closing handler and opening a new one")
			bh.closeHandler(rec.Sample)
			bh.createHandler(ctx, rec.Sample)
		}
		bh.mutex.Unlock()
	}
}

// createHandlers create handlers to GCP storage mapped to the sample ID
func (bh *BucketHandler) createHandlers(ctx context.Context) {
	ch := make(chan int, 10)
	go func() {
		for sampleID := range ch {
			bh.createHandler(ctx, sampleID)
		}
	}()

	for i := 0; i < fileHandlerCount; i++ {
		ch <- i
	}
	close(ch)
}

func (bh *BucketHandler) createHandler(ctx context.Context, sampleID int) {
	date := time.Now().UTC().Format("20060102")
	// file path: fast-truck/date=20220430/smpl=13/20220430_134130_00000015.parquet
	path := fmt.Sprintf(
		"%s/date=%s/smpl=%d/%s_%08d.parquet",
		bh.bucketDir, date, sampleID, time.Now().UTC().Format("20060102_150405"), sampleID)
	fw := bh.bucket.Object(path).NewWriter(ctx)
	pw, err := writer.NewParquetWriterFromWriter(fw, new(model.AdRecord), int64(parquetRoutines))
	if err != nil {
		log.Println("Can't create parquet writer", path, err)
		bh.errCh <- err
	}
	pw.RowGroupSize = groupSize
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	bh.fileHandlers[sampleID] = &parquetHandler{
		fw:   fw,
		pw:   pw,
		size: 0,
	}
}

// closeHandlers close GCP storage handlers
func (bh *BucketHandler) closeHandlers() {
	ch := make(chan int, 10)

	go func() {
		for sampleID := range ch {
			bh.closeHandler(sampleID)
		}
	}()

	for i := 0; i < fileHandlerCount; i++ {
		ch <- i
	}
	close(ch)
}

func (bh *BucketHandler) closeHandler(sampleID int) {
	err := bh.fileHandlers[sampleID].pw.WriteStop()
	if err != nil {
		log.Println("Can't close parquet writer", err)
	}
	err = bh.fileHandlers[sampleID].fw.Close()
	if err != nil {
		log.Println("Can't close file writer", err)
	}
	bh.fileHandlers[sampleID] = nil
}
