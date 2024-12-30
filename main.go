package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
)

func main() {
	//Accept a file location as input
	var fileLocation string
	var numberOfParts int
	var bucketName string

	flag.StringVar(&fileLocation, "filepath", "", "Location of the file")
	flag.StringVar(&bucketName, "bucket", "", "Cloud Storage bucket name")
	flag.IntVar(&numberOfParts, "num-of-parts", 5, "Number of splits you'd like for you file with a maximum of 32")
	flag.Parse()

	if fileLocation == "" {
		fmt.Println("You must pass a location for your file")
		os.Exit(-1)
	}

	// Read file and split into N parts
	fileInfo, err := os.Stat(fileLocation)
	if err != nil {
		log.Printf("Unable to get metadata for file %s", err)
		os.Exit(-1)
	}
	fileName := filepath.Base(fileLocation)
	extension := filepath.Ext(fileLocation)

	sizeOfEachPart := fileInfo.Size() / int64(numberOfParts)
	remainder := fileInfo.Size() % int64(numberOfParts)

	pool := &sync.Pool{
		New: func() any {
			return make([]byte, sizeOfEachPart)
		},
	}

	// Create GCS client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("Error creating GCS client %s", err)
		os.Exit(-1)
	}
	bkt := client.Bucket(bucketName)

	f, err := os.Open(fileLocation)
	if err != nil {
		log.Printf("Unable to access file %s", err)
		os.Exit(-1)
	}

	parts := []*storage.ObjectHandle{}
	for i := 0; i < numberOfParts; i++ {
		parts = append(parts, nil)
	}
	eg, egCtx := errgroup.WithContext(ctx)

	for i := int64(0); i < int64(numberOfParts); i++ {
		eg.Go(func() error {
			var buf []byte
			if i != int64(numberOfParts)-1 {
				buf = pool.Get().([]byte)
			} else {
				buf = make([]byte, sizeOfEachPart+remainder)
			}

			bytesRead, err := f.ReadAt(buf, i*sizeOfEachPart)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				log.Printf("Unable to read part %d", i)
				return err
			}

			obj := bkt.Object(fmt.Sprintf("%s-%d.%s", fileName, i, extension))
			bufReader := bytes.NewBuffer(buf[:bytesRead])
			w := obj.NewWriter(egCtx)
			defer w.Close()

			_, err = io.Copy(w, bufReader)
			if err != nil {
				return err
			}

			parts[i] = obj

			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		log.Printf("Unable to get all parts %s", err)
		os.Exit(-1)
	}

	obj := bkt.Object(fileInfo.Name())
	composer := obj.ComposerFrom(parts...)
	_, err = composer.Run(ctx)
	if err != nil {
		log.Printf("Unable to consolidate parts %s", err)
		os.Exit(-1)
	}
}
