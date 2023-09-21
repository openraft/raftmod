/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import "github.com/hashicorp/serf/client"

type SerfCommand interface {

	/**
	Full description of the command
	*/
	Help() string

	/**
	Sub command name
	*/

	SubCommand() string

	/**
	Run sub command
	*/

	Run(client *client.RPCClient, args []string) error

	/**
	One line description of the command
	*/
	Synopsis() string

}

