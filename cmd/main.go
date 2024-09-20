package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
)

const inputDir = "input/"

func processFiles() {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		log.Printf("error reading directory: %v\n", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := inputDir + entry.Name()
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("error reading file %s: %v\n", filePath, err)
			continue
		}

		fileName := entry.Name()
		log.Printf("Processing file: %s", fileName)

		batches := batchFile(data)

		for _, batchID := range batches {
			batchData, err := getBatchData(batchID)
			if err != nil {
				log.Printf("Error reading batch %s: %v", batchID, err)
				continue
			}

			log.Printf("Sending batch %s for file %s (size: %d bytes)", batchID, fileName, len(batchData))
			_, client := getMasterNode()
			batch_dest := getBatchDest()
			success, worker_id := sendBatch(batch_dest, batchID, batchData)
			if success {
				if err := cleanupBatch(batchID); err != nil {
					log.Printf("Error cleaning up batch %s: %v", batchID, err)
				}
			} else {
				log.Printf("Failed to send batch %s for file %s", batchID, fileName)
			}
		}
		os.Remove(filePath);
	}
}

func getBatchDest() uuid.UUID {

}

func batchFile(data []byte) []uuid.UUID {

}

func getBatchData(batchID uuid.UUID) ([]byte, error) {

}

func getMasterNode() (string, string) {

}
func cleanupBatch(bID uuid.UUID) error{

}
func sendBatch(client uuid.UUID, batchID uuid.UUID, bData []byte) (bool, string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &pb.BatchRequest{
		BatchId:   &pb.UUID{Value: batchID.String()},
		BatchData: bData,
	}

	log.Printf("Sending batch request for batch ID: %s", batchID)
	res, err := client.SendBatch(ctx, req)
	if err != nil {
		log.Printf("Error sending batch: %v", err)
		return false, ""
	}

	log.Printf("Batch sent successfully. Worker ID: %s", res.WorkerId.Value)
	return res.Success, res.WorkerId.Value
}

func main() {
	fmt.Println("client module.")
}
