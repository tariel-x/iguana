package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tariel-x/iguana/internal/proxy"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "address",
				Value:   "localhost:9092",
				EnvVars: []string{"ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "broker",
				Value:   "localhost:9093",
				EnvVars: []string{"BROKER"},
			},
		},
		Action: listen,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func listen(c *cli.Context) error {
	address := c.String("address")
	broker := c.String("broker")
	p, err := proxy.NewProxy(address, broker)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sig:
			log.Println(fmt.Sprintf("stopping by OS signal (%v)", s))
			cancel()
		case <-ctx.Done():
		}
	}()

	log.Println("proxy started")
	return p.Start(ctx)
}
