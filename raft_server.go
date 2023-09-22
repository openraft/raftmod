/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"crypto/tls"
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/sprintframework/raftapi"
	"github.com/pkg/errors"
	"github.com/sprintframework/sprint"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
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

	SerfAddress       string       `value:"serf-server.listen-address,default="`
	SerfQueueSize     int          `value:"serf-server.queue-size,default=2048"`

	//SerfConfig   *serf.Config `inject`
	//serf         *serf.Serf
	//serfChLAN    chan  serf.Event

	// reconcileCh is used to pass events from the serf handler
	// into the leader manager, so that the strong state can be
	// updated
	// reconcileCh chan serf.Member

	// should be defined by application
	FSM      raft.FSM   `inject`

	RaftAddress  string          `value:"raft-server.listen-address,default="`
	MaxPool      int             `value:"raft-server.max-pool,default=3"`
	Timeout      time.Duration   `value:"raft-server.timeout,default=10s"`

	listener  net.Listener
	transport *raft.NetworkTransport

	raft      *raft.Raft

	alive        atomic.Bool
	shutdownOnce sync.Once
	shutdownCh   chan struct{}

}

func RaftServer() raftapi.RaftServer {
	return &implRaftServer{
		shutdownCh:  make(chan struct{}),
	}
}

func (t *implRaftServer) PostConstruct() error {
	//t.serfChLAN = make(chan serf.Event, t.SerfQueueSize)
	//t.SerfConfig.EventCh = t.serfChLAN
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

	raftAddr, err := ParseAndAdjustTCPAddr(t.RaftAddress, t.NodeService.NodeSeq())
	if err != nil {
		return errors.Errorf("issue in property 'raft-server.listen-address', %v", err)
	}
	t.RaftAddress = fmt.Sprintf("%s:%d", raftAddr.IP.String(), raftAddr.Port)

	t.listener, err = net.Listen("tcp", t.RaftAddress)
	if err != nil {
		return errors.Errorf("bind failed on '%s', %v", t.RaftAddress, err)
	}

	advertise, err := net.ResolveTCPAddr("tcp", ReplaceToPrivateIP(t.RaftAddress))
	if err != nil {
		return errors.Errorf("tcp address resolve '%s', %v", t.listener.Addr().String(), err)
	}

	t.Log.Info("RaftServerFactory", zap.String("bind", t.listener.Addr().String()), zap.String("advertise", advertise.String()))

	t.transport, err = newTCPTransport(t.listener, advertise, t.TlsConfig, func(stream raft.StreamLayer) *raft.NetworkTransport {
		config := &raft.NetworkTransportConfig{Stream: stream, MaxPool: t.MaxPool, Timeout: t.Timeout, Logger: t.HCLog.Named("raft-transport"),
			ServerAddressProvider: t.ServerLookup}
		return raft.NewNetworkTransportWithConfig(config)

		//return raft.NewNetworkTransport(stream, t.MaxPool, t.Timeout, os.Stderr)
	})
	if err != nil {
		return errors.Errorf("raft transport creation error for address '%s', %v", advertise.String(), err)
	}

	return nil
}

func (t *implRaftServer) Alive() bool {
	return t.alive.Load()
}

func (t *implRaftServer) Transport() (raft.Transport, bool) {
	return t.transport, t.transport != nil
}

func (t *implRaftServer) Raft() (*raft.Raft, bool) {
	return t.raft, t.raft != nil
}

func (t *implRaftServer) IsLeader() bool {
	return t.alive.Load() && t.raft.State() == raft.Leader
}

func (t *implRaftServer) ListenAddress() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	} else {
		return sprint.EmptyAddr
	}
}

func (t *implRaftServer) Serve() (err error) {

	panicToError(&err)

	t.Log.Info("RaftServerServe", zap.String("addr", t.RaftAddress), zap.Bool("tls", t.TlsConfig != nil))

	t.alive.Store(false)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(t.NodeService.NodeIdHex())
	config.Logger = t.HCLog.Named("raft")

	t.raft, err = raft.NewRaft(config, t.FSM, t.LogStore, t.StableStore, t.FileSnapshotStore, t.transport)
	if err != nil {
		return err
	}

	/*
	t.serf, err = serf.Create(t.SerfConfig)
	if err != nil {
		t.Log.Error("SerfCreate", zap.String("action", "shutdown raft"), zap.Error(err))
		t.raft.Shutdown()
		return err
	}

	for _, m := range t.serf.Members() {
		t.Log.Info("Member", zap.Any("member", m))
		server, err := ParseServerTags(m, t.Application.Name())
		if err != nil {
			t.Log.Debug("ParseServerTags", zap.Any("member", m), zap.Error(err))
			continue
		}
		t.ServerLookup.AddServer(server)
	}

	serfAddr := fmt.Sprintf("%s:%d", t.SerfConfig.MemberlistConfig.BindAddr, t.SerfConfig.MemberlistConfig.BindPort)
	t.Log.Info("SerfServerServe", zap.String("addr", serfAddr), zap.Any("stats", t.serf.Stats()))
	 */

	t.alive.Store(true)
	return nil
}

func (t *implRaftServer) Shutdown() {
	t.alive.Store(false)

	t.shutdownOnce.Do(func() {

		t.Log.Info("RaftServerShutdown", zap.String("addr", t.RaftAddress))
		/*
		if t.serf != nil {
			if err := t.serf.Leave(); err != nil {
				t.Log.Error("SerfLeave", zap.Error(err))
			}
			if err := t.serf.Shutdown(); err != nil {
				t.Log.Error("SerfShutdown", zap.Error(err))
			}
		}
		 */
		if t.raft != nil {
			future := t.raft.Shutdown()
			go func() {
				if err := future.Error(); err != nil {
					t.Log.Error("RaftShutdown", zap.Error(err))
				}
			}()
		}
		if t.transport != nil {
			t.transport.Close()
		}
		if t.listener != nil {
			t.listener.Close()
		}
		close(t.shutdownCh)
	})
}

func (t *implRaftServer) ShutdownCh() <-chan struct{} {
	return t.shutdownCh
}

func (t *implRaftServer) Destroy() error {
	t.Shutdown()
	return nil
}

