package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	pb "github.com/razvanmarinn/rcss/proto"

	"google.golang.org/grpc"
)

type RCSSClient struct {
	UserId        int
	CurrentMaster string
}

type Metadata struct {
	BatchIDs      []string
	BatchLocation map[string]string
}

func NewRCSSClient() *RCSSClient {
	return &RCSSClient{
		UserId:        rand.Intn(100),
		CurrentMaster: "localhost:50051",
	}
}

// This should fetch metadata from somewhere (e.g., master node, database)
func (rc *RCSSClient) GetMetadata(fileName string) Metadata {
	// Dummy data for the sake of the example
	return Metadata{
		BatchIDs: []string{"batch1", "batch2"},
		BatchLocation: map[string]string{
			"batch1": "worker1:5000",
			"batch2": "worker2:5000",
		},
	}
}

// Assuming this method gets the gRPC client for the worker
func (rc *RCSSClient) GetWorkerClient(location string) (pb.WorkerClient, error) {
	// Initialize gRPC connection and client (this is a dummy example)
	conn, err := grpc.Dial(location, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker: %v", err)
	}
	return pb.NewWorkerClient(conn), nil
}

func (rc *RCSSClient) GetFileBackFromWorkers(fileName string) ([]byte, error) {
	metadata := rc.GetMetadata(fileName)
	fileData := make([]byte, 0)

	for _, batchID := range metadata.BatchIDs {
		// Get the worker location for the current batch
		location, exists := metadata.BatchLocation[batchID]
		if !exists {
			return nil, errors.New("batch location not found")
		}

		// Fetch worker client
		client, err := rc.GetWorkerClient(location)
		if err != nil {
			return nil, fmt.Errorf("failed to get worker client: %v", err)
		}

		// Create a context with timeout for the gRPC request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Fetch the batch data from the worker
		resp, err := client.GetBatch(ctx, &pb.Ttt{BatchID: batchID})
		if err != nil {
			return nil, fmt.Errorf("failed to get batch data from worker: %v", err)
		}

		// Append the data to the fileData slice
		fileData = append(fileData, resp.BatchData...)
	}

	return fileData, nil
}
