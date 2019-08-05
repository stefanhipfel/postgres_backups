package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/docker/docker/client"
	"github.com/namsral/flag"
	"github.com/robfig/cron"
	"github.com/stefanhipfel/postgres-backup/pkg/backup"
	"github.com/stefanhipfel/postgres-backup/pkg/writer"
)

var cli *client.Client
var ctx context.Context
var cronTime string
var writeTo string

func init() {
	flag.StringVar(&cronTime, "cron_time", "@daily", "set the cron job time")
	flag.StringVar(&writeTo, "write_to", "file", "where the backup should be written to")
	flag.Parse()
}

func main() {
	var err error
	cr := cron.New()
	ctx = context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	p, err := backup.NewPostgres("tastycard-backend_db_1")
	s3, _ := writer.NewS3("eu-central-1", "tastycard")

	err = cr.AddFunc(cronTime, func() { p.Backup(ctx, s3) })
	if err != nil {
		panic(err)
	}
	cr.Start()

	defer func() {
		signal.Stop(c)
		cr.Stop()
		cancel()
	}()

	select {
	case <-c:
		cancel()
	case <-ctx.Done():
	}
}
