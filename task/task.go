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
func (c *Task) Run(ctx context.Context) (*Result, error) {
	err := reindex(ctx)
	if err != nil {
		return nil, err
	}

	return &Result{
		Success: true,
	}, nil
}
