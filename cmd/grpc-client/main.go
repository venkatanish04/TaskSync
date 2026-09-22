package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/venkatanish04/tasksync/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const serverAddress = "localhost:50051"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskSyncServiceClient(conn)

	fmt.Println("=== TaskSync gRPC Client ===")

	fmt.Println("\n1. Creating task...")
	putResponse, err := client.PutTask(ctx, &pb.PutTaskRequest{
		Task: &pb.Task{
			Id:          "task-101",
			Title:       "Learn Go",
			Description: "Study Go concurrency and networking",
			Status:      "Pending",
		},
	})
	if err != nil {
		log.Fatalf("PutTask failed: %v", err)
	}
	fmt.Println("Success:", putResponse.GetSuccess())
	fmt.Println("Message:", putResponse.GetMessage())

	fmt.Println("\n2. Getting task...")
	getResponse, err := client.GetTask(ctx, &pb.GetTaskRequest{Id: "task-101"})
	if err != nil {
		log.Fatalf("GetTask failed: %v", err)
	}
	if getResponse.GetFound() {
		task := getResponse.GetTask()
		fmt.Println("ID:", task.GetId())
		fmt.Println("Title:", task.GetTitle())
		fmt.Println("Description:", task.GetDescription())
		fmt.Println("Status:", task.GetStatus())
	} else {
		fmt.Println("Task not found")
	}

	fmt.Println("\n3. Updating task...")
	updateResponse, err := client.PutTask(ctx, &pb.PutTaskRequest{
		Task: &pb.Task{
			Id:          "task-101",
			Title:       "Learn Advanced Go",
			Description: "Study Go concurrency, gRPC and distributed systems",
			Status:      "In Progress",
		},
	})
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Println("Success:", updateResponse.GetSuccess())

	fmt.Println("\n4. Getting updated task...")
	getResponse, err = client.GetTask(ctx, &pb.GetTaskRequest{Id: "task-101"})
	if err != nil {
		log.Fatalf("GetTask failed: %v", err)
	}
	if getResponse.GetFound() {
		task := getResponse.GetTask()
		fmt.Println("ID:", task.GetId())
		fmt.Println("Title:", task.GetTitle())
		fmt.Println("Description:", task.GetDescription())
		fmt.Println("Status:", task.GetStatus())
	}

	fmt.Println("\n5. Deleting task...")
	deleteResponse, err := client.DeleteTask(ctx, &pb.DeleteTaskRequest{Id: "task-101"})
	if err != nil {
		log.Fatalf("DeleteTask failed: %v", err)
	}
	fmt.Println("Success:", deleteResponse.GetSuccess())
	fmt.Println("Message:", deleteResponse.GetMessage())

	fmt.Println("\n6. Checking deleted task...")
	getResponse, err = client.GetTask(ctx, &pb.GetTaskRequest{Id: "task-101"})
	if err != nil {
		log.Fatalf("GetTask failed: %v", err)
	}
	if !getResponse.GetFound() {
		fmt.Println("Task successfully deleted")
	} else {
		fmt.Println("Task still exists")
	}

	fmt.Println("\n=== Test completed ===")
}
