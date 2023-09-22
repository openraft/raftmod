/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"github.com/go-errors/errors"
	"github.com/hashicorp/serf/serf"
	"github.com/sprintframework/raftapi"
	"net"
	"strconv"
)


func ParseServerTags(m serf.Member, role string) (*raftapi.Server, error) {
	if m.Tags["role"] != role {
		return nil, errors.Errorf("joining role '%s' whereas expected role '%s'", m.Tags["role"], role)
	}

	portStr := m.Tags["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, errors.Errorf("parsing 'port' tag '%s', %v", portStr, err)
	}

	raftStr := m.Tags["raft-port"]
	raftPort, err := strconv.Atoi(raftStr)
	if err != nil {
		return nil, errors.Errorf("parsing 'raft-port' tag '%s', %v", raftStr, err)
	}

	grpcStr := m.Tags["grpc-port"]
	grpcPort, err := strconv.Atoi(grpcStr)
	if err != nil {
		return nil, errors.Errorf("parsing 'grpc-port' tag '%s', %v", grpcStr, err)
	}

	addr := &net.TCPAddr{IP: m.Addr, Port: port}

	server := &raftapi.Server{
		Name:                m.Name,
		ID:                  m.Tags["id"],
		Port:                port,
		JoinPort:            int(m.Port),
		RaftPort:            raftPort,
		RPCPort:             grpcPort,
		Addr:                addr,
		Build:               m.Tags["build"],
		Version:             m.Tags["version"],
		Status:              m.Status.String(),
	}
	return server, nil
}
