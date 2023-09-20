/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

import (
	"github.com/codeallergy/glue"
	"github.com/openraft/raftbadger"
	"github.com/keyvalstore/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"reflect"
)

var StableStoreClass = reflect.TypeOf((*raft.StableStore)(nil)).Elem()

type implRaftStableStoreFactory struct {

	RaftStore     store.ManagedDataStore    `inject:"bean=raft-storage"`
	RaftConfPrefix string `value:"raft-storage.stable-prefix,default=conf"`
}

func RaftStableStoreFactory() glue.FactoryBean {
	return &implRaftStableStoreFactory{}
}

func (t *implRaftStableStoreFactory) Object() (object interface{}, err error) {

	defer panicToError(&err)

	db, ok := t.RaftStore.Instance().(*badger.DB)
	if !ok {
		return nil, errors.Errorf("managed data delegate 'raft-storage' must have badger backend")
	}

	return raftbadger.NewStableStore(db, []byte(t.RaftConfPrefix)), nil

}

func (t *implRaftStableStoreFactory) ObjectType() reflect.Type {
	return StableStoreClass
}

func (t *implRaftStableStoreFactory) ObjectName() string {
	return "raft-storage-stable"
}

func (t *implRaftStableStoreFactory) Singleton() bool {
	return true
}
