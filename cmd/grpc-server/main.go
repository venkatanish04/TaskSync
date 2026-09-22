package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	grpcserver "github.com/venkatanish04/tasksync/internal/grpc"
	"github.com/venkatanish04/tasksync/internal/lsm"
	pb "github.com/venkatanish04/tasksync/proto"

	"google.golang.org/grpc"
)

const (
	serverAddress = ":50051"
	dataDirectory = "./data/node1"
)

func main() {
	log.Println("Starting TaskSync gRPC server...")

	store, err := lsm.Open(dataDirectory, 5)
	if err != nil {
		log.Fatalf("failed to open LSM storage: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("failed to close storage: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", serverAddress, err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterTaskSyncServiceServer(grpcServer, grpcserver.NewServer(store))

	go func() {
		signalChannel := make(chan os.Signal, 1)
		signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

		<-signalChannel
		log.Println("Shutting down TaskSync gRPC server...")
		grpcServer.GracefulStop()
	}()

	log.Printf("TaskSync gRPC server listening on %s", serverAddress)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
