package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/prometheus/common/log"
	"github.com/stefanhipfel/postgres-backup/pkg/writer"
)

var mutex = &sync.Mutex{}

type Postgres struct {
	docker *client.Client
	name   string
}

func NewPostgres(name string) (p *Postgres, err error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return p, err
	}

	return &Postgres{
		docker: cli,
		name:   name,
	}, err
}

func (p *Postgres) Backup(ctx context.Context, w writer.Writer) (err error) {
	var c []types.Container
	c, err = p.getContainer(ctx)
	if err != nil {
		return
	}

	cfg := types.ExecConfig{
		AttachStdout: true,
		AttachStderr: true,
		User:         "root",
		Cmd: []string{
			"pg_dumpall", "-c", "-U", "postgres",
		},
	}

	log.Infof("Running BACKUP for postgres container id: %s \n", c[0].ID)
	execID, err := p.docker.ContainerExecCreate(context.Background(), c[0].ID, cfg)
	if err != nil {
		return
	}
	config := types.ExecStartCheck{}
	res, err := p.docker.ContainerExecAttach(context.Background(), execID.ID, config)
	if err != nil {
		return
	}

	err = p.docker.ContainerExecStart(context.Background(), execID.ID, types.ExecStartCheck{})
	if err != nil {
		return
	}

	dt := time.Now()
	w.Write(fmt.Sprintf("%s.gz", dt.Format("01-02-2006 15_04_05")), res.Reader)

	if err != nil {
		return
	}

	return
}

// Reload the list of running containers
func (p *Postgres) getContainer(ctx context.Context) (c []types.Container, err error) {
	log.Infoln("Reloading containers")
	filters := filters.NewArgs()
	filters.Add("name", "tastycard-backend_db_1")

	mutex.Lock()
	defer mutex.Unlock()
	c, err = p.docker.ContainerList(ctx, types.ContainerListOptions{
		All:     false,
		Filters: filters,
	})

	if err != nil {
		return c, err
	}

	if len(c) != 1 || !strings.Contains(c[0].Status, "healthy") {
		c = nil
		return c, errors.New("postgres container not healthy")
	}

	return c, err
}

/*
// Listen for docker events
func listen() {
	filter := filters.NewArgs()
	filter.Add("type", "container")
	filter.Add("event", "start")
	filter.Add("event", "die")

	msg, errChan := cli.Events(context.Background(), types.EventsOptions{
		Filters: filter,
	})

	for {
		select {
		case err := <-errChan:
			panic(err)
		case e := <-msg:
			log.Infoln("NEW EVENT :", e.ID, e.Status)
			if e.Status == "die" {
				PgContainer = nil
			} else {
				if err := util.Retry(10, time.Duration(10)*time.Second, reload); err != nil {
					log.Errorln(err.Error())
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
*/
