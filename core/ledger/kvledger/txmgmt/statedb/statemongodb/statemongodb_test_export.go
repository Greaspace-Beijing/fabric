/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statemongodb

import (
	"testing"

	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

// TestVDBEnv provides a mongodb backed versioned db for testing
type TestVDBEnv struct {
	t          testing.TB
	DBProvider statedb.VersionedDBProvider
}

// NewTestVDBEnv instantiates and new mongodb backed TestVDB
func NewTestVDBEnv(t testing.TB) *TestVDBEnv {
	t.Logf("Creating new TestVDBEnv")

	dbProvider, _ := NewVersionedDBProvider(&disabled.Provider{})
	testVDBEnv := &TestVDBEnv{t, dbProvider}
	// No cleanup for new test environment.  Need to cleanup per test for each DB used in the test.
	return testVDBEnv
}

// Cleanup drops the test mongo databases and closes the db provider
func (env *TestVDBEnv) Cleanup() {
	env.t.Logf("Cleaning up TestVDBEnv")
	CleanupDB(env.t, env.DBProvider)

	env.DBProvider.Close()
}

func CleanupDB(t testing.TB, dbProvider statedb.VersionedDBProvider) {
	mongodbProvider, _ := dbProvider.(*VersionedDBProvider)
	for _, v := range mongodbProvider.databases {
		if err := v.metadataDB.DropCollection(); err != nil {
			assert.Failf(t, "DropCollection %s fails. err: %v", v.metadataDB.CollectionName, err)
		}

		for _, db := range v.namespaceDBs {
			if err := db.DropCollection(); err != nil {
				assert.Failf(t, "DropCollection %s fails. err: %v", db.CollectionName, err)
			}
		}
	}
}
