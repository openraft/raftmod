/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
	"strings"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

func (t *implRaftServer) eventHandlerLAN() {
	for {
		select {
		case e := <-t.serfChLAN:
			switch e.EventType() {
			case serf.EventMemberJoin:
				t.nodeJoinLAN(e.(serf.MemberEvent))
				t.localMemberEvent(e.(serf.MemberEvent))

			case serf.EventMemberLeave, serf.EventMemberFailed, serf.EventMemberReap:
				t.nodeFailedLAN(e.(serf.MemberEvent))
				t.localMemberEvent(e.(serf.MemberEvent))

			case serf.EventUser:
				t.localEvent(e.(serf.UserEvent))
			case serf.EventMemberUpdate:
				t.nodeUpdateLAN(e.(serf.MemberEvent))
				t.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventQuery: // Ignore
			default:
				t.Log.Warn("UnknownSerfEvent", zap.String("network", "LAN"), zap.Any("event", e))
			}

		case <-t.shutdownCh:
			return
		}
	}
}

func (t *implRaftServer) nodeJoinLAN(me serf.MemberEvent) {
	for _, m := range me.Members {
		server, err := ParseServerTags(m, t.Application.Name())
		if err != nil {
			t.Log.Debug("SerfNodeJoinLAN", zap.Error(err))
			continue
		}
		t.Log.Info("SerfNodeJoinLAN", zap.String("server", server.String()))

		// Update server lookup
		t.ServerLookup.AddServer(server)
	}
}

func (t *implRaftServer) nodeUpdateLAN(me serf.MemberEvent) {
	for _, m := range me.Members {
		server, err := ParseServerTags(m, t.Application.Name())
		if err != nil {
			t.Log.Debug("SerfNodeUpdateLAN", zap.Error(err))
			continue
		}
		t.Log.Info("SerfNodeUpdateLAN", zap.String("server", server.String()))

		t.ServerLookup.AddServer(server)
	}
}

func (t *implRaftServer) nodeFailedLAN(me serf.MemberEvent) {
	for _, m := range me.Members {
		server, err := ParseServerTags(m, t.Application.Name())
		if err != nil {
			t.Log.Debug("SerfNodeFailedLAN", zap.Error(err))
			continue
		}
		t.Log.Info("SerfNodeFailedLAN", zap.String("server", server.String()))

		// Update id to address map
		t.ServerLookup.RemoveServer(server)
	}
}

func (t *implRaftServer) localMemberEvent(me serf.MemberEvent) {
	/*
	// Do nothing if we are not the leader
	if !t.IsLeader() {
		return
	}

	// Check if this is a reap event
	isReap := me.EventType() == serf.EventMemberReap

	// Queue the members for reconciliation
	for _, m := range me.Members {
		// Change the status if this is a reap event
		if isReap {
			m.Status = StatusReap
		}
		select {
		case t.reconcileCh <- m:
		default:
		}
	}
	*/
}

func (t *implRaftServer) localEvent(event serf.UserEvent) {

	prefix := t.Application.Name() + ":"
	if !strings.HasPrefix(event.Name,  prefix) {
		return
	}
	eventName := event.Name[len(prefix):]

	if eventName == "new-leader" {
		t.Log.Info("NewLeaderElected", zap.String("payload", string(event.Payload)))
	}

}

