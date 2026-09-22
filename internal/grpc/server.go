package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/venkatanish04/tasksync/internal/lsm"
	"github.com/venkatanish04/tasksync/internal/task"
	pb "github.com/venkatanish04/tasksync/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the TaskSync gRPC service.
type Server struct {
	pb.UnimplementedTaskSyncServiceServer

	store *lsm.LSM
}

// NewServer creates a new gRPC server.
func NewServer(store *lsm.LSM) *Server {
	return &Server{
		store: store,
	}
}

// PutTask creates or updates a task.
func (s *Server) PutTask(
	ctx context.Context,
	req *pb.PutTaskRequest,
) (*pb.PutTaskResponse, error) {

	if req == nil || req.GetTask() == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"task is required",
		)
	}

	pbTask := req.GetTask()

	if pbTask.GetId() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"task id is required",
		)
	}

	if pbTask.GetTitle() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"task title is required",
		)
	}

	internalTask := task.Task{
		ID:          pbTask.GetId(),
		Title:       pbTask.GetTitle(),
		Description: pbTask.GetDescription(),
		Status:      pbTask.GetStatus(),
	}

	data, err := json.Marshal(internalTask)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			"failed to encode task",
		)
	}

	if err := s.store.Put(
		internalTask.ID,
		string(data),
	); err != nil {
		log.Printf("PutTask failed: %v", err)

		return nil, status.Error(
			codes.Internal,
			"failed to store task",
		)
	}

	return &pb.PutTaskResponse{
		Success: true,
		Message: "task stored successfully",
	}, nil
}

// GetTask retrieves a task.
func (s *Server) GetTask(
	ctx context.Context,
	req *pb.GetTaskRequest,
) (*pb.GetTaskResponse, error) {

	if req == nil || req.GetId() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"task id is required",
		)
	}

	value, err := s.store.Get(req.GetId())

	if err != nil {
		if errors.Is(err, lsm.ErrKeyNotFound) {
			return &pb.GetTaskResponse{Found: false}, nil
		}

		log.Printf("GetTask failed: %v", err)

		return nil, status.Error(
			codes.Internal,
			"failed to retrieve task",
		)
	}

	var internalTask task.Task

	if err := json.Unmarshal([]byte(value), &internalTask); err != nil {
		log.Printf("failed to decode task: %v", err)

		return nil, status.Error(
			codes.Internal,
			"failed to decode stored task",
		)
	}

	return &pb.GetTaskResponse{
		Found: true,
		Task: &pb.Task{
			Id:          internalTask.ID,
			Title:       internalTask.Title,
			Description: internalTask.Description,
			Status:      internalTask.Status,
		},
	}, nil
}

// DeleteTask deletes a task.
func (s *Server) DeleteTask(
	ctx context.Context,
	req *pb.DeleteTaskRequest,
) (*pb.DeleteTaskResponse, error) {

	if req == nil || req.GetId() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"task id is required",
		)
	}

	_, err := s.store.Get(req.GetId())

	if err != nil {
		if errors.Is(err, lsm.ErrKeyNotFound) {
			return &pb.DeleteTaskResponse{
				Success: false,
				Message: "task not found",
			}, nil
		}

		return nil, status.Error(
			codes.Internal,
			"failed to check task",
		)
	}

	if err := s.store.Delete(req.GetId()); err != nil {
		log.Printf("DeleteTask failed: %v", err)

		return nil, status.Error(
			codes.Internal,
			"failed to delete task",
		)
	}

	return &pb.DeleteTaskResponse{
		Success: true,
		Message: "task deleted successfully",
	}, nil
}
