/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"fmt"
	"github.com/hashicorp/serf/client"
	"github.com/hashicorp/serf/serf"
	"github.com/hashicorp/serf/version"
	"github.com/sprintframework/sprint"
	"strings"
)

type serfVersionCommand struct {
	Application  sprint.Application   `inject`
}

func SerfVersionCommand() SerfCommand {
	return &serfVersionCommand{}
}

func (t serfVersionCommand) Help() string {
	helpText := `
Usage: serf version

  Prints the Serf version.

`
	return strings.TrimSpace(helpText)
}

func (t serfVersionCommand) SubCommand() string {
	return "version"
}

func (t serfVersionCommand) Synopsis() string {
	return "Prints the Serf version"
}

func (t serfVersionCommand) Run(client *client.RPCClient, args []string) error {
	println(version.GetHumanVersion())
	fmt.Printf("Agent Protocol: %d (Understands back to: %d)\n",
		serf.ProtocolVersionMax, serf.ProtocolVersionMin)
	return nil
}


