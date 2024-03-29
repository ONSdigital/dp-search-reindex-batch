package task

import (
	"context"

	"github.com/ONSdigital/dp-search-reindex-batch/config"
)

// Task defines a runnable task
type Task struct {
	Config *config.Config
}

// Result holds final results of a task run
type Result struct {
	Success bool
	Err     error
}

// Run runs the task
func (t *Task) Run(ctx context.Context) *Result {
	err := reindex(ctx, t.Config)
	if err != nil {
		return &Result{
			Success: false,
			Err:     err,
		}
	}

	return &Result{
		Success: true,
	}
}
