/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mongodb

import (
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGetMongoDBDefinition(t *testing.T) {
	expectedUrl := viper.GetString("ledger.state.mongoDBConfig.mongoDBAddress")

	testutil.SetupCoreYAMLConfig()
	mongoDBDef := GetMongoDBDefinition()
	assert.Equal(t, expectedUrl, mongoDBDef.URL)
	assert.Equal(t, "", mongoDBDef.Username)
	assert.Equal(t, "", mongoDBDef.Password)
	assert.Equal(t, 3, mongoDBDef.MaxRetries)
	assert.Equal(t, 12, mongoDBDef.MaxRetriesOnStartup)
	assert.Equal(t, time.Second*35, mongoDBDef.RequestTimeout)
}
