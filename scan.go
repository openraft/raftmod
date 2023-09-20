/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftmod

var Scan = []interface{}{
	RaftLogStoreFactory(),
	RaftStableStoreFactory(),
	RaftSnapshotFactory(),
	SerfConfigFactory(),
	ServerLookup(),
	RaftServer(),
	RaftClientPool(),
}
