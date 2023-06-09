package task

import (
	"context"
)

// Task defines a runnable task
type Task struct {
}

// Result holds final results of a task run
type Result struct {
	Success bool
}

// Run runs the task
func (c *Task) Run(_ context.Context) (*Result, error) {
	return &Result{
		Success: true,
	}, nil
}
