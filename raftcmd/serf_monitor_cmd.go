/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"flag"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/logutils"
	"github.com/hashicorp/serf/client"
	"go.uber.org/atomic"
	"os"
	"os/signal"
	"strings"
)

type serfMonitorCommand struct {
	quitting atomic.Bool
}

func SerfMonitorCommand() SerfCommand {
	return &serfMonitorCommand{}
}

func (t serfMonitorCommand) Help() string {
	helpText := `
Usage: serf monitor [options]

  Shows recent log messages of a Serf agent, and attaches to the agent,
  outputting log messages as they occur in real time. The monitor lets you
  listen for log levels that may be filtered out of the Serf agent. For
  example your agent may only be logging at INFO level, but with the monitor
  you can see the DEBUG level logs.

Options:

  -log-level=info          Log level of the agent.
`
	return strings.TrimSpace(helpText)
}


func (t serfMonitorCommand) SubCommand() string {
	return "monitor"
}

func (t serfMonitorCommand) Synopsis() string {
	return "Stream logs from a Serf agent"
}

func (t serfMonitorCommand) Run(prov ClientProvider, args []string) error {
	var logLevel string
	cmdFlags := flag.NewFlagSet("monitor", flag.ContinueOnError)
	cmdFlags.Usage = func() { println(t.Help()) }
	cmdFlags.StringVar(&logLevel, "log-level", "INFO", "log level")

	if err := cmdFlags.Parse(args); err != nil {
		return err
	}

	return prov.DoWithClient(func(cli *client.RPCClient) error {
		return t.doRun(cli, logLevel)
	})
}

func (t serfMonitorCommand) doRun(client *client.RPCClient, logLevel string) error {

	eventCh := make(chan map[string]interface{}, 1024)
	streamHandle, err := client.Stream("*", eventCh)
	if err != nil {
		return errors.Errorf("starting stream, %v", err)
	}
	defer client.Stop(streamHandle)

	logCh := make(chan string, 4096)
	monHandle, err := client.Monitor(logutils.LogLevel(logLevel), logCh)
	if err != nil {
		return errors.Errorf("starting monitor, %v", err)
	}
	defer client.Stop(monHandle)

	shutdownCh := makeShutdownCh()

	eventDoneCh := make(chan struct{})
	go func() {
		defer close(eventDoneCh)
	OUTER:
		for {
			select {
			case log := <-logCh:
				if log == "" {
					break OUTER
				}
				println(log)
			case event := <-eventCh:
				if event == nil {
					break OUTER
				}
				println("Event Info:")
				for key, val := range event {
					fmt.Printf("\t%s: %#v\n", key, val)
				}
			}
		}

		if !t.quitting.Load() {
			println("")
			println("Remote side ended the monitor.")
		}
	}()

	select {
	case <-eventDoneCh:
		return nil
	case <-shutdownCh:
		t.quitting.Store(true)
	}

	return nil
}

func makeShutdownCh() <-chan struct{} {
	resultCh := make(chan struct{})

	signalCh := make(chan os.Signal, 10)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		for {
			<-signalCh
			resultCh <- struct{}{}
		}
	}()

	return resultCh
}
