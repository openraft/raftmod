/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"flag"
	"fmt"
	"github.com/hashicorp/serf/client"
	"github.com/pkg/errors"
	"strings"
)

type serfEventCommand struct {
}

func SerfEventCommand() SerfCommand {
	return &serfEventCommand{}
}

func (t serfEventCommand) SubCommand() string {
	return "event"
}

func (t serfEventCommand) Help() string {
	helpText := `
Usage: serf event [options] name payload

  Dispatches a custom event across the Serf cluster.

Options:

  -coalesce=true/false      Whether this event can be coalesced. This means
                            that repeated events of the same name within a
                            short period of time are ignored, except the last
                            one received. Default is true.
`
	return strings.TrimSpace(helpText)
}

func (t serfEventCommand) Synopsis() string {
	return "Emit a custom event through the Serf cluster"
}

func (t serfEventCommand) Run(prov ClientProvider, args []string) error {

	var coalesce bool

	cmdFlags := flag.NewFlagSet("event", flag.ContinueOnError)
	cmdFlags.Usage = func() { println(t.Help()) }
	cmdFlags.BoolVar(&coalesce, "coalesce", true, "coalesce")

	if err := cmdFlags.Parse(args); err != nil {
		return err
	}

	args = cmdFlags.Args()
	if len(args) < 1 {
		return errors.Errorf("an event name must be specified\n%s", t.Help())
	} else if len(args) > 2 {
		return errors.Errorf("too many command line arguments\n%s", t.Help())
	}

	event := args[0]
	payload := []byte(args[1])

	return prov.DoWithClient(func(cli *client.RPCClient) error {
		return t.doRun(cli, event, payload, coalesce)
	})
}

func (t serfEventCommand) doRun(client *client.RPCClient, event string, payload []byte, coalesce bool) error {

	if err := client.UserEvent(event, payload, coalesce); err != nil {
		return errors.Errorf("sending event '%s', %v", event, err)
	}

	fmt.Printf("Event '%s' dispatched! Coalescing enabled: %#v", event, coalesce)
	return nil
}


