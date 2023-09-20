/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"fmt"
	"github.com/codeallergy/glue"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
	"github.com/sprintframework/sprint"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
)

var SerfConfigClass = reflect.TypeOf((*serf.Config)(nil)).Elem()

type implSerfConfigFactory struct {

	Properties      glue.Properties     `inject`

	Application     sprint.Application  `inject`
	NodeService     sprint.NodeService  `inject`

	SerfAddress  string            `value:"raft-server.serf-address,default="`
	RaftAddress  string            `value:"raft-server.listen-address,default="`
	RPCBean      string            `value:"raft-server.rpc-bean,default="`

	DataDir           string       `value:"application.data.dir,default="`
	DataDirPerm       os.FileMode  `value:"application.perm.data.dir,default=-rwxrwx---"`
	DataFilePerm      os.FileMode  `value:"application.perm.data.file,default=-rw-rw-r--"`

}

func SerfConfigFactory() glue.FactoryBean {
	return &implSerfConfigFactory{}
}

func (t *implSerfConfigFactory) Object() (object interface{}, err error) {

	defer panicToError(&err)

	dataDir := t.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.Application.ApplicationDir(), "db")

		if err := createDirIfNeeded(dataDir, t.DataDirPerm); err != nil {
			return nil, err
		}

		dataDir = filepath.Join(dataDir, t.Application.Name())
	}

	if err := createDirIfNeeded(dataDir, t.DataDirPerm); err != nil {
		return nil, err
	}

	snapshotPath := filepath.Join(dataDir, "serf.snapshot")

	if err := createDirIfNeeded(snapshotPath, t.DataDirPerm); err != nil {
		return nil, err
	}

	conf := serf.DefaultConfig()
	conf.Init()

	conf.NodeName = t.NodeService.NodeIdHex()
	conf.SnapshotPath = snapshotPath

	conf.Tags["id"] = t.NodeService.NodeIdHex()
	conf.Tags["role"] = t.Application.Name()
	conf.Tags["version"] = t.Application.Version()
	conf.Tags["build"] = t.Application.Build()

	if t.SerfAddress == "" {
		return nil, errors.New("required property 'raft-server.serf-address' is empty")
	}

	serfHost, serfPort, err := getHostAndPortNumber(t.SerfAddress)
	if err != nil {
		return nil, errors.Errorf("invalid port in property 'raft-server.serf-address', %v", err)
	}
	if serfHost == "" {
		serfHost = "0.0.0.0"
	}

	memberConfig := conf.MemberlistConfig

	memberConfig.BindAddr = serfHost
	memberConfig.BindPort = serfPort
	memberConfig.AdvertisePort = serfPort

	conf.Tags["port"] = strconv.Itoa(serfPort)

	if t.RaftAddress != "" {
		raftPort, err := getPortNumber(t.RaftAddress)
		if err != nil {
			return nil, errors.Errorf("invalid port in property 'raft-server.listen-address', %v", err)
		}
		conf.Tags["raft-port"] = strconv.Itoa(raftPort)
	}

	if t.RPCBean != "" {
		propName := fmt.Sprintf("%s.%s", t.RPCBean, "listen-address")
		value := t.Properties.GetString(propName, "")
		if value == "" {
			return nil, errors.Errorf("empty property '%s' needed by 'raft-server.rpc-bean' reference", propName)
		}
		rpcPort, err := getPortNumber(value)
		if err != nil {
			return nil, errors.Errorf("invalid port in property '%s', %v", propName, err)
		}
		conf.Tags["grpc-port"] = strconv.Itoa(rpcPort)
	}

	return conf, nil
}

func (t *implSerfConfigFactory) ObjectType() reflect.Type {
	return SerfConfigClass
}

func (t *implSerfConfigFactory) ObjectName() string {
	return "serf-config"
}

func (t *implSerfConfigFactory) Singleton() bool {
	return true
}

