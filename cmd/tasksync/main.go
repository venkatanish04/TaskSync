package main

import (
	"fmt"
	"log"

	"github.com/venkatanish04/tasksync/internal/storage"
	"github.com/venkatanish04/tasksync/internal/task"
)

func main() {
	store := storage.NewStore()

	newTask := task.Task{
		ID:          "101",
		Title:       "Learn Go",
		Description: "Learn Go for distributed systems",
		Status:      "pending",
	}

	// PUT
	store.Put(newTask)

	fmt.Println("Task inserted successfully")

	// GET
	result, err := store.Get("101")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Task retrieved:")
	fmt.Printf("%+v\n", result)

	// DELETE
	err = store.Delete("101")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Task deleted successfully")

	fmt.Println("Remaining tasks:", store.Count())
}
