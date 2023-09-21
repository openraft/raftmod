/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/serf/client"
	"github.com/sprintframework/sprint"
	"strings"
)

type serfRttCommand struct {
	Application  sprint.Application   `inject`
}

func SerfRttCommand() SerfCommand {
	return &serfRttCommand{}
}

func (t serfRttCommand) Help() string {
	helpText := `
Usage: serf rtt [options] node1 [node2]

  Estimates the round trip time between two nodes using Serf's network
  coordinate model of the cluster.

  At least one node name is required. If the second node name isn't given, it
  is set to the agent's node name. Note that these are node names as known to
  Serf as "serf members" would show, not IP addresses.

`
	return strings.TrimSpace(helpText)
}

func (t serfRttCommand) SubCommand() string {
	return "rtt"
}

func (t serfRttCommand) Synopsis() string {
	return "Estimates network round trip time between nodes"
}

func (t serfRttCommand) Run(client *client.RPCClient, args []string) error {

	nodes := args

	if len(nodes) == 1 {
		stats, err := client.Stats()
		if err != nil {
			return errors.Errorf("querying agent, %v", err)
		}
		nodes = append(nodes, stats["agent"]["name"])
	} else if len(nodes) != 2 {
		return errors.Errorf("one or two node names must be specified\n%s", t.Help())
	}

	// Get the coordinates.
	coord1, err := client.GetCoordinate(nodes[0])
	if err != nil {
		return errors.Errorf("getting coordinates, %v", err)
	}

	if coord1 == nil {
		return errors.Errorf("could not find a coordinate for node %q", nodes[0])
	}

	coord2, err := client.GetCoordinate(nodes[1])
	if err != nil {
		return errors.Errorf("getting coordinates, %v", err)
	}

	if coord2 == nil {
		return errors.Errorf("could not find a coordinate for node %q", nodes[1])
	}

	// Report the round trip time.
	dist := fmt.Sprintf("%.3f ms", coord1.DistanceTo(coord2).Seconds()*1000.0)
	fmt.Printf("Estimated %s <-> %s rtt: %s\n", nodes[0], nodes[1], dist)
	return nil
}


