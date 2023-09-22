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
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
)

var SerfConfigClass = reflect.TypeOf((*serf.Config)(nil))

type implSerfConfigFactory struct {

	Log             *zap.Logger         `inject`
	Properties      glue.Properties     `inject`

	Application     sprint.Application  `inject`
	NodeService     sprint.NodeService  `inject`

	SerfAddress  string            `value:"serf-server.listen-address,default="`
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

		dataDir = filepath.Join(dataDir, t.NodeService.NodeName())
	}

	if err := createDirIfNeeded(dataDir, t.DataDirPerm); err != nil {
		return nil, err
	}

	snapshotFolder := filepath.Join(dataDir, "serf")

	if err := createDirIfNeeded(snapshotFolder, t.DataDirPerm); err != nil {
		return nil, err
	}

	conf := serf.DefaultConfig()
	conf.Init()

	conf.NodeName = t.NodeService.NodeName()
	conf.SnapshotPath = filepath.Join(snapshotFolder, "local.snapshot")

	conf.Logger = zap.NewStdLog(t.Log.Named("serf"))
	
	conf.Tags["id"] = t.NodeService.NodeIdHex()
	conf.Tags["role"] = t.Application.Name()
	conf.Tags["version"] = t.Application.Version()
	conf.Tags["build"] = t.Application.Build()

	if t.SerfAddress == "" {
		return nil, errors.New("required property 'serf-server.listen-address' is empty")
	}

	tcpAddr, err := ParseTCPAddr(t.SerfAddress)
	if err != nil {
		return nil, errors.Errorf("issue in property 'serf-server.listen-address', %v", err)
	}

	memberConfig := conf.MemberlistConfig

	memberConfig.BindAddr = tcpAddr.IP.String()
	memberConfig.BindPort = tcpAddr.Port

	conf.Tags["port"] = strconv.Itoa(tcpAddr.Port)

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

