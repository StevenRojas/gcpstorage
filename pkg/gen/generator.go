package gen

import (
	"fmt"
	"github.com/StevenRojas/gcpstorage/pkg/model"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"math/rand"
)

func Generate(recordCh chan model.AdRecord, count int) {
	for i := 0; i < count; i++ {
		rec := model.AdRecord{
			ID:     uuid.New().String(),
			Name:   fmt.Sprintf("Ad # %d", i),
			Value:  50.0 + float32(i)*0.1,
			Reason: fmt.Sprintf("Reason Ad # %d", i),
			Sample: rand.Intn(10),
			Size:   100 + i,
		}
		log.Println(rec)
		recordCh <- rec
	}
	// close(recordCh)
}

var parquetSchema = `
    {
        "Tag":"name=test",
        "Fields":[
		    {"Tag":"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"},
		    {"Tag":"name=age, type=INT32"},
		    {"Tag":"name=id, type=INT64"},
		    {"Tag":"name=weight, type=FLOAT"}
        ]
	}
`

func generate(num int) {
	var err error

	//write
	fw, err := local.NewLocalFileWriter("json.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewJSONWriter(parquetSchema, fw, 4)
	if err != nil {
		log.Println("Can't create json writer", err)
		return
	}

	for i := 0; i < num; i++ {
		rec := `
            {
                "name":"%s-%d",
                "age":%d,
                "id":%d,
                "weight":%f,
                "sex":%t
            }
        `

		rec = fmt.Sprintf(rec, "Student Name", i, 20+i%5, i, 50.0+float32(i)*0.1, i%2 == 0)
		if err = pw.Write(rec); err != nil {
			log.Println("Write error", err)
		}

	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	err = fw.Close()
	if err != nil {
		log.Println("Write Finished with error", err)
	}
	log.Println("Write Finished")
}
