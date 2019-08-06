package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/docker/docker/client"
	"github.com/joho/godotenv"
	"github.com/namsral/flag"
	"github.com/prometheus/common/log"
	"github.com/robfig/cron"
	"github.com/stefanhipfel/postgres-backup/pkg/backup"
	"github.com/stefanhipfel/postgres-backup/pkg/writer"
)

var (
	cli      *client.Client
	ctx      context.Context
	cronTime string
	writeTo  string
)

func init() {
	err := godotenv.Load("/env/.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	flag.StringVar(&cronTime, "CRON_TIME", "@daily", "set the cron job time")
	flag.StringVar(&writeTo, "WRITE_TO", "file", "where the backup should be written to")
	flag.Parse()
}

func main() {
	var err error
	var wr writer.Writer
	cr := cron.New()
	ctx = context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	p, err := backup.NewPostgres("tastycard-backend_db_1")
	if writeTo == "S3" {
		wr, _ = writer.NewS3("eu-central-1", "tastycard")
	} else {
		wr, _ = writer.NewFile("./")
	}

	err = cr.AddFunc(cronTime, func() { p.Backup(ctx, wr) })
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
