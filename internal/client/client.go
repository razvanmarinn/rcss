package client

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	pb "github.com/razvanmarinn/rcss/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMsgSize = 100 * 1024 * 1024

type RCSSClient struct {
	UserId        int
	CurrentMaster string
}

func NewRCSSClient() *RCSSClient {
	return &RCSSClient{
		UserId:        rand.Intn(100),
		CurrentMaster: "localhost:50055",
	}
}

func (rc *RCSSClient) GetMetadata(fileName string) (*pb.MasterMetadataResponse, error) {
	conn, err := grpc.Dial(rc.CurrentMaster, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.Location{FileName: fileName}
	resp, err := client.GetMetadata(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}

	return resp, nil
}

func (rc *RCSSClient) GetWorkerClient(location string) (pb.WorkerServiceClient, error) {
	conn, err := grpc.Dial(location,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker: %v", err)
	}
	return pb.NewWorkerServiceClient(conn), nil
}

func (rc *RCSSClient) GetFileBackFromWorkers(fileName string) ([]byte, error) {
	metadata, err := rc.GetMetadata(fileName)
	if err != nil {
		return nil, err
	}

	fileData := make([]byte, 0)
	for _, batchID := range metadata.Batches {
		location := metadata.BatchLocations[batchID]
		client, err := rc.GetWorkerClient(location.WorkerIds[0]) // to modify in the future ( where replicas come into play)
		if err != nil {
			return nil, fmt.Errorf("failed to get worker client: %v, %s", err, client)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
		defer cancel()

		resp, err := client.GetBatch(ctx, &pb.Ttt{BatchID: batchID})
		if err != nil {
			return nil, fmt.Errorf("failed to get batch data from worker: %v", err)
		}

		fileData = append(fileData, resp.BatchData...)
	}

	return fileData, nil
}

// Process file by sending it to the master node for distribution to workers
func (rc *RCSSClient) ProcessFileToMaster(fileName string, fileContent []byte) error {
	batchID := uuid.New()
	conn, err := grpc.Dial(rc.CurrentMaster, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	req := &pb.ClientBatchRequestToMaster{
		BatchId:   batchID.String(),
		BatchSize: fmt.Sprintf("%d", len(fileContent)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	res, err := client.GetBatchDestination(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get worker destination: %v", err)
	}

	workerClient, err := rc.GetWorkerClient(fmt.Sprintf("%s:%d", res.WorkerIp, res.WorkerPort))
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %v", err)
	}

	sendReq := &pb.ClientRequestToWorker{
		BatchId: batchID.String(),
		Data:    fileContent,
	}

	_, err = workerClient.SendBatch(ctx, sendReq)
	if err != nil {
		return fmt.Errorf("failed to send batch to worker: %v", err)
	}

	return nil
}
