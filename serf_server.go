/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"crypto/tls"
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/go-errors/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/serf/cmd/serf/command/agent"
	"github.com/hashicorp/serf/serf"
	"github.com/openraft/raftapi"
	"github.com/sprintframework/sprint"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net"
	"sync"
)

type implSerfServer struct {

	Properties      glue.Properties     `inject`
	Log             *zap.Logger         `inject`
	HCLog           hclog.Logger        `inject`
	TlsConfig       *tls.Config         `inject:"optional"`
	NodeService     sprint.NodeService  `inject`

	SerfConfig      *serf.Config        `inject`
	agentConfig     *agent.Config

	listener        net.Listener
	serfAgent       *agent.Agent
	ipc             *agent.AgentIPC

	EventHandlers   []agent.EventHandler   `inject`

	RPCAddress     string     `value:"serf-server.rpc-address,default=:8700"`
	RPCAuthKey     string     `value:"serf-server.rpc-auth,default="`

	alive        atomic.Bool
	shutdownOnce sync.Once
	shutdownCh   chan struct{}

}

func SerfRPCServer() raftapi.SerfServer {
	return &implSerfServer{
		shutdownCh:  make(chan struct{}),
	}
}

func (t *implSerfServer) PostConstruct() (err error) {
	t.agentConfig = agent.DefaultConfig()
	t.agentConfig.BindAddr = fmt.Sprintf("%s:%d", t.SerfConfig.MemberlistConfig.BindAddr, t.SerfConfig.MemberlistConfig.BindPort)
	t.agentConfig.RPCAddr = t.RPCAddress
	t.agentConfig.RPCAuthKey = t.RPCAuthKey
	t.agentConfig.EnableCompression = t.SerfConfig.MemberlistConfig.EnableCompression
	t.agentConfig.Tags = t.SerfConfig.Tags

	t.serfAgent, err = agent.Create(t.agentConfig, t.SerfConfig, t.SerfConfig.LogOutput)
	if err != nil {
		return errors.Errorf("failed to create the Serf agent, %v", err)
	}

	for _, eh := range t.EventHandlers {
		t.Log.Info("RegisterEventHandler", zap.Any("eh", eh))
		t.serfAgent.RegisterEventHandler(eh)
	}

	return nil
}

func (t *implSerfServer) BeanName() string {
	return "serf-rpc-server"
}

func (t *implSerfServer) GetStats(cb func(name, value string) bool) error {
	return nil
}

func (t *implSerfServer) Bind() error {

	tcpAddr, err := ParseAndAdjustTCPAddr(t.agentConfig.RPCAddr, t.NodeService.NodeSeq())
	if err != nil {
		return err
	}
	t.RPCAddress = fmt.Sprintf("%s:%d", tcpAddr.IP.String(), tcpAddr.Port)
	t.agentConfig.RPCAddr = t.RPCAddress

	// Setup the RPC listener
	t.listener, err = net.Listen("tcp", t.agentConfig.RPCAddr)
	if err != nil {
		return errors.Errorf("failed to bind on address '%s', %v", t.agentConfig.RPCAddr, err)
	}

	return nil
}

func (t *implSerfServer) Alive() bool {
	return t.alive.Load()
}

func (t *implSerfServer) ListenAddress() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	} else {
		return sprint.EmptyAddr
	}
}

func (t *implSerfServer) Serve() (err error) {

	panicToError(&err)

	t.Log.Info("SerfRPCServerServe", zap.String("addr", t.RPCAddress), zap.Bool("tls", t.TlsConfig != nil))

	t.alive.Store(false)
	err = t.serfAgent.Start()
	if err != nil {
		return err
	}

	t.ipc = agent.NewAgentIPC(t.serfAgent, t.RPCAuthKey, t.listener, t.SerfConfig.LogOutput, agent.NewLogWriter(512))
	t.alive.Store(true)

	return nil
}

func (t *implSerfServer) Shutdown() {
	t.alive.Store(false)

	t.shutdownOnce.Do(func() {

		t.Log.Info("SerfServerShutdown", zap.String("addr", t.RPCAddress))

		if t.ipc != nil {
			t.ipc.Shutdown()
		}
		if t.serfAgent != nil {

			if err := t.serfAgent.Serf().Leave(); err != nil {
				t.Log.Error("SerfLeave", zap.Error(err))
			}

			t.serfAgent.Shutdown()
		}
		if t.listener != nil {
			t.listener.Close()
		}
		close(t.shutdownCh)
	})

}

func (t *implSerfServer) Config() (*serf.Config, bool) {
	return t.SerfConfig, t.SerfConfig != nil
}

func (t *implSerfServer) Serf() (*serf.Serf, bool) {
	return t.serfAgent.Serf(), t.serfAgent != nil
}

func (t *implSerfServer) Agent() (*agent.Agent, bool) {
	return t.serfAgent, t.serfAgent != nil
}

func (t *implSerfServer) ShutdownCh() <-chan struct{} {
	return t.shutdownCh
}

func (t *implSerfServer) Destroy() error {
	t.Shutdown()
	return nil
}

