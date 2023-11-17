package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-search-reindex-batch/config"
	"github.com/ONSdigital/dp-search-reindex-batch/task"
)

const serviceName = "dp-search-reindex-batch"

var (
	// BuildTime represents the time in which the service was built
	BuildTime string
	// GitCommit represents the commit (SHA-1) hash of the service that is running
	GitCommit string
	// Version represents the version of the service that is running
	Version string
)

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	if err := run(ctx); err != nil {
		log.Fatal(ctx, "fatal runtime error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	// Read config
	cfg, err := config.Get()
	if err != nil {
		return errors.Wrap(err, "unable to retrieve service configuration")
	}
	log.Info(ctx, "config on startup", log.Data{"config": cfg, "build_time": BuildTime, "git-commit": GitCommit})

	chk := task.Task{
		Config: cfg,
	}

	ctx, ctxCancelFunc := context.WithCancel(ctx)

	// Run the task in the background, using a result channel
	resultChan := make(chan *task.Result, 1)
	go func() {
		resultChan <- chk.Run(ctx)
	}()

	// blocks until an os interrupt, task completion or a fatal error occurs
	select {
	case sig := <-signals:
		log.Info(ctx, "os signal received", log.Data{"signal": sig})
	case result := <-resultChan:
		log.Info(ctx, "task result", log.Data{"Success": result.Success})
		if result.Err != nil {
			log.Error(ctx, "task error received", err)
			ctxCancelFunc()
			return err
		}
		log.Info(ctx, "task complete")
	}
	return nil
}
