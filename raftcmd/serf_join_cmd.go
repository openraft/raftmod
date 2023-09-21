/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"flag"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/serf/client"
	"strings"
)

type serfJoinCommand struct {
}

func SerfJoinCommand() SerfCommand {
	return &serfJoinCommand{}
}

func (t serfJoinCommand) Help() string {
	helpText := `
Usage: serf join [options] address ...

  Tells a running Serf agent (with "serf agent") to join the cluster
  by specifying at least one existing member.

Options:

  -replay                   Replay past user events.
`
	return strings.TrimSpace(helpText)
}

func (t serfJoinCommand) SubCommand() string {
	return "join"
}

func (t serfJoinCommand) Synopsis() string {
	return "Tell Serf agent to join cluster"
}

func (t serfJoinCommand) Run(client *client.RPCClient, args []string) error {
	var replayEvents bool

	cmdFlags := flag.NewFlagSet("join", flag.ContinueOnError)
	cmdFlags.Usage = func() { println(t.Help()) }
	cmdFlags.BoolVar(&replayEvents, "replay", false, "replay past user events")

	if err := cmdFlags.Parse(args); err != nil {
		return err
	}

	args = cmdFlags.Args()
	if len(args) == 0 {
		return errors.Errorf("at least one address to join must be specified\n%s", t.Help())
	}

	n, err := client.Join(args, replayEvents)
	if err != nil {
		return errors.Errorf("joining the cluster '%+v', %v", args, err)
	}

	fmt.Printf("Successfully joined cluster by contacting %d nodes.\n", n)
	return nil
}


