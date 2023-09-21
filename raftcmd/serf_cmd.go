/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/hashicorp/serf/client"
	"github.com/pkg/errors"
	"github.com/sprintframework/sprint"
	"sort"
	"strings"
)

type serfCommand struct {
	Application  sprint.Application   `inject`
	Context      glue.Context         `inject`

	// keep it sorted by SubCommand()
	SerfCommands   []SerfCommand `inject`

	SerfAddress   string    `value:"serf.addr,default=127.0.0.1:7373"`
	SerfToken     string    `value:"serf.auth,default="`

}

func SerfCommands() sprint.Command {
	return &serfCommand{}
}

func (t *serfCommand) BeanName() string {
	return "serf"
}

func (t *serfCommand) PostConstruct() error {
	sort.Slice(t.SerfCommands, func(i, j int) bool {
		left, right := t.SerfCommands[i].SubCommand(), t.SerfCommands[j].SubCommand()
		return left < right
	})
	return nil
}

func (t *serfCommand) findCommand(key string) (SerfCommand, bool) {
	n := len(t.SerfCommands)
	i := sort.Search(n, func(i int) bool {
		return t.SerfCommands[i].SubCommand() >= key
	})
	if i == n {
		return nil, false
	} else if t.SerfCommands[i].SubCommand() == key {
		return t.SerfCommands[i], true
	} else {
		return nil, false
	}
}


func (t *serfCommand) Help() string {
	helpText := `
Usage: ./%s serf [command]

	Provides management functionality for the Serf (gossip) server.

Commands:

%s
`
	var sb strings.Builder
	for _, cmd := range t.SerfCommands {
		sb.WriteString(fmt.Sprintf("    %s           %s\n", cmd.SubCommand(), cmd.Synopsis()))
	}

	return strings.TrimSpace(fmt.Sprintf(helpText, t.Application.Executable(), sb.String()))
}

func (t *serfCommand) Run(args []string) error {

	if len(args) == 0 {
		println(t.Help())
		return nil
	}

	cmd := args[0]
	args = args[1:]

	if handler, ok := t.findCommand(cmd); ok {
		return t.doRun(handler, args)
	} else {
		return errors.Errorf("unknown sub command '%s' for serf, Usage: ./%s serf [%s]",
			cmd, t.Application.Name(), t.subCommands())
	}
}

func (t *serfCommand) doRun(handler SerfCommand, args []string) error {
	config := client.Config{Addr: t.SerfAddress, AuthKey: t.SerfToken}
	client, err := client.ClientFromConfig(&config)
	if err != nil {
		return errors.Errorf("Error connecting to Serf agent: %s", err)
	}
	defer client.Close()
	return handler.Run(client, args)
}

func (t *serfCommand) subCommands() string {
	var sub []string
	for _, cmd := range t.SerfCommands {
		sub = append(sub, cmd.SubCommand())
	}
	return strings.Join(sub, ",")
}

func (t *serfCommand) Synopsis() string {

	var sub []string
	for _, cmd := range t.SerfCommands {
		sub = append(sub, cmd.SubCommand())
	}

	return fmt.Sprintf("serf commands [%s]", t.subCommands())
}

