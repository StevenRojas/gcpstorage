package main

import (
	"context"
	"flag"
	"github.com/StevenRojas/gcpstorage/pkg/buckets"
	"github.com/StevenRojas/gcpstorage/pkg/gen"
	"github.com/pkg/profile"
	"log"
	"time"
)

var (
	projectID  = flag.String("project_id", "mad-hack", "Cloud Project ID, used for session creation.")
	bucketName = flag.String("bucket_name", "1st-task-team", "Cloud bucket name.")
	bucketDir  = flag.String("bucket_dir", "fast-truck", "Cloud bucket base directory.")
)

func main() {
	// defer profile.Start().Stop()
	defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath(".")).Stop()
	flag.Parse()
	ctx := context.Background()

	if *projectID == "" {
		log.Fatalf("No parent project ID specified, please supply using the --project_id flag.")
	}
	if *bucketName == "" {
		log.Fatalf("No bucket name specified, please supply using the --bucket_name flag.")
	}
	if *bucketDir == "" {
		log.Fatalf("No bucket directory specified, please supply using the --bucket_dir flag.")
	}

	bucketHandler, err := buckets.NewBucketHandler(ctx, *projectID, *bucketName, *bucketDir)
	if err != nil {
		log.Fatalf(err.Error())
	}
	recordsCh, errCh := bucketHandler.StartWorkers(ctx)
	// defer bucketHandler.Close()

	select {
	case e := <-errCh:
		log.Println("error: ", e)
	default:
	}

	gen.Generate(recordsCh, 1000)

	time.Sleep(1 * time.Minute)
}
