/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftcmd

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hashicorp/serf/client"
	"sort"
	"strings"
)

type serfInfoCommand struct {
}

func SerfInfoCommand() SerfCommand {
	return &serfInfoCommand{}
}

func (t serfInfoCommand) Help() string {
	helpText := `
Usage: serf info [options]

	Provides debugging information for operators

Options:

  -format                  If provided, output is returned in the specified
                           format. Valid formats are 'json', and 'text' (default)
`
	return strings.TrimSpace(helpText)
}

func (t serfInfoCommand) SubCommand() string {
	return "info"
}

func (t serfInfoCommand) Synopsis() string {
	return "Provides debugging information for operators"
}

func (t serfInfoCommand) Run(client *client.RPCClient, args []string) error {

	var format string
	cmdFlags := flag.NewFlagSet("info", flag.ContinueOnError)
	cmdFlags.Usage = func() { println(t.Help()) }
	cmdFlags.StringVar(&format, "format", "text", "output format")

	if err := cmdFlags.Parse(args); err != nil {
		return err
	}

	stats, err := client.Stats()
	if err != nil {
		return err
	}

	output, err := formatOutput(statsString(stats), format)
	if err != nil {
		return errors.Errorf("encoding error: %s", err)
	}

	println(output)
	return nil
}

func statsString(s map[string]map[string]string) string {
	var buf bytes.Buffer

	// Get the keys in sorted order
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Iterate over each top-level key
	for _, key := range keys {
		buf.WriteString(fmt.Sprintf(key + ":\n"))

		// Sort the sub-keys
		subvals := s[key]
		subkeys := make([]string, 0, len(subvals))
		for k := range subvals {
			subkeys = append(subkeys, k)
		}
		sort.Strings(subkeys)

		// Iterate over the subkeys
		for _, subkey := range subkeys {
			val := subvals[subkey]
			buf.WriteString(fmt.Sprintf("\t%s = %s\n", subkey, val))
		}
	}
	return buf.String()
}


func formatOutput(data interface{}, format string) ([]byte, error) {
	var out string

	switch format {

	case "json":
		jsonBin, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return nil, err
		}
		out = string(jsonBin)

	case "text":
		out = data.(fmt.Stringer).String()

	default:
		return nil, errors.Errorf("invalid output format \"%s\"", format)

	}
	return []byte(strings.TrimSpace(out)), nil
}



