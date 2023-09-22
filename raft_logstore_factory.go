/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"github.com/codeallergy/glue"
	"github.com/keyvalstore/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/sprintframework/raft-badger"
	"reflect"
)

var LogStoreClass = reflect.TypeOf((*raft.LogStore)(nil)).Elem()

type implRaftLogStoreFactory struct {

	RaftStore     store.ManagedDataStore    `inject:"bean=raft-storage"`
	RaftLogPrefix string `value:"raft-storage.log-prefix,default=log"`

}

func RaftLogStoreFactory() glue.FactoryBean {
	return &implRaftLogStoreFactory{}
}

func (t *implRaftLogStoreFactory) Object() (object interface{}, err error) {

	defer panicToError(&err)

	db, ok := t.RaftStore.Instance().(*badger.DB)
	if !ok {
		return nil, errors.New("managed data delegate 'raft-storage' must have badger backend")
	}

	return raftbadger.NewLogStore(db, []byte(t.RaftLogPrefix)), nil

}

func (t *implRaftLogStoreFactory) ObjectType() reflect.Type {
	return LogStoreClass
}

func (t *implRaftLogStoreFactory) ObjectName() string {
	return "raft-storage-log"
}

func (t *implRaftLogStoreFactory) Singleton() bool {
	return true
}
