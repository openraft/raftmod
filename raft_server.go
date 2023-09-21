/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"crypto/tls"
	"github.com/codeallergy/glue"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/openraft/raftapi"
	"github.com/pkg/errors"
	"github.com/sprintframework/sprint"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net"
	"time"
)

const (
	// reconcileChSize is the size of the buffered channel reconcile updates from Serf.
	// If this is exhausted we will drop updates, and wait for a periodic reconcile.
	reconcileChSize = 256
)

type implRaftServer struct {

	Properties      glue.Properties     `inject`
	Log             *zap.Logger         `inject`
	HCLog           hclog.Logger        `inject`
	TlsConfig       *tls.Config         `inject:"optional"`

	Application     sprint.Application  `inject`
	NodeService     sprint.NodeService  `inject`

	LogStore           raft.LogStore       `inject`
	StableStore        raft.StableStore    `inject`
	FileSnapshotStore  raft.SnapshotStore  `inject`

	ServerLookup       raftapi.ServerLookup  `inject`

	SerfAddress       string       `value:"raft-server.serf-address,default="`
	SerfQueueSize     int          `value:"raft-server.serf-queue-size,default=2048"`

	SerfConfig   *serf.Config `inject`
	cluster      *serf.Serf
	serfListener net.Listener
	serfChLAN    chan  serf.Event

	// reconcileCh is used to pass events from the serf handler
	// into the leader manager, so that the strong state can be
	// updated
	reconcileCh chan serf.Member

	// should be defined by application
	FSM      raft.FSM   `inject`

	RaftAddress  string          `value:"raft-server.listen-address,default="`
	MaxPool      int             `value:"raft-server.max-pool,default=3"`
	Timeout      time.Duration   `value:"raft-server.timeout,default=10s"`

	listener  net.Listener
	transport *raft.NetworkTransport

	raft      *raft.Raft

	running   atomic.Bool
	shutdownCh chan struct{}

}

func RaftServer() raftapi.RaftServer {
	return &implRaftServer{
		reconcileCh: make(chan serf.Member, reconcileChSize),
		shutdownCh:  make(chan struct{}),
	}
}

func (t *implRaftServer) PostConstruct() error {
	t.serfChLAN = make(chan serf.Event, t.SerfQueueSize)
	t.SerfConfig.EventCh = t.serfChLAN
	return nil
}

func (t *implRaftServer) BeanName() string {
	return "raft-server"
}

func (t *implRaftServer) GetStats(cb func(name, value string) bool) error {
	if t.raft != nil {
		for k, v := range t.raft.Stats() {
			cb(k, v)
		}
	}
	return nil
}

func (t *implRaftServer) Bind() (err error) {

	if t.RaftAddress == "" {
		t.Log.Warn("RaftAddressEmpty", zap.String("prop", "raft-server.listen-address"))
		return nil
	}

	if t.SerfAddress == "" {
		t.Log.Warn("SerfAddressEmpty", zap.String("prop", "raft-server.serf-address"))
		return nil
	}

	t.RaftAddress = addLocalIP(t.RaftAddress)
	t.SerfAddress = addLocalIP(t.SerfAddress)

	t.listener, err = net.Listen("tcp", t.RaftAddress)
	if err != nil {
		return errors.Errorf("bind failed on '%s', %v", t.RaftAddress, err)
	}

	t.serfListener, err = net.Listen("tcp", t.SerfAddress)
	if err != nil {
		return errors.Errorf("bind failed on '%s', %v", t.SerfAddress, err)
	}

	advertise, err := net.ResolveTCPAddr("tcp", t.listener.Addr().String())
	if err != nil {
		return errors.Errorf("tcp address resolve '%s', %v", t.listener.Addr().String(), err)
	}

	t.transport, err = newTCPTransport(t.listener, advertise, t.TlsConfig, func(stream raft.StreamLayer) *raft.NetworkTransport {
		config := &raft.NetworkTransportConfig{Stream: stream, MaxPool: t.MaxPool, Timeout: t.Timeout, Logger: t.HCLog.Named("raft"),
			ServerAddressProvider: t.ServerLookup}
		return raft.NewNetworkTransportWithConfig(config)

		//return raft.NewNetworkTransport(stream, t.MaxPool, t.Timeout, os.Stderr)
	})
	if err != nil {
		return errors.Errorf("raft transport creation error for address '%s', %v", advertise.String(), err)
	}

	return nil
}

func (t *implRaftServer) Active() bool {
	return t.running.Load()
}

func (t *implRaftServer) Transport() (raft.Transport, bool) {
	return t.transport, t.running.Load()
}

func (t *implRaftServer) Raft() (*raft.Raft, bool) {
	return t.raft, t.running.Load()
}

func (t *implRaftServer) ListenAddress() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	} else {
		return EmptyAddr{}
	}
}

func (t *implRaftServer) Serve() (err error) {

	panicToError(&err)

	t.Log.Info("RaftServerServe", zap.String("addr", t.RaftAddress), zap.Bool("tls", t.TlsConfig != nil))

	t.running.Store(true)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(t.NodeService.NodeIdHex())

	t.raft, err = raft.NewRaft(config, t.FSM, t.LogStore, t.StableStore, t.FileSnapshotStore, t.transport)
	if err != nil {
		return err
	}

	t.cluster, err = serf.Create(t.SerfConfig)
	if err != nil {
		return err
	}

	t.running.Store(true)
	return nil
}

func (t *implRaftServer) Stop() {
	t.running.Store(false)
	if t.running.CompareAndSwap(true, false) {
		if t.cluster != nil {
			t.cluster.Shutdown()
		}
		if t.raft != nil {
			t.raft.Shutdown()
		}
		if t.transport != nil {
			t.transport.Close()
		}
		if t.serfListener != nil {
			t.serfListener.Close()
		}
		if t.listener != nil {
			t.listener.Close()
		}
	}
}

func (t *implRaftServer) Destroy() error {
	t.Stop()
	return nil
}

func (t *implRaftServer) IsLeader() bool {
	return t.raft != nil && t.raft.State() == raft.Leader
}

