///*
//Copyright IBM Corp. All Rights Reserved.
//SPDX-License-Identifier: Apache-2.0
//*/
//
package mongodb

import (
	"encoding/hex"
	"testing"

	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/common/util"
	"github.com/stretchr/testify/assert"
)

//Unit test of mongodb util functionality
func TestCreateMongoDBConnectionAndDB(t *testing.T) {

	collection := "testcreatemongodbconnectionanddb"
	cleanupCollection(collection)
	defer cleanupCollection(collection)
	//create a new connection
	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	assert.NoError(t, err, "Error when trying to CreateMongoInstance")

	_, err = CreateMongoCollection(mongoInstance, collection)
	assert.NoError(t, err, "Error when trying to CreateMongoCollection")
}

func TestCollectionMapping(t *testing.T) {
	//create a new instance and collection object using a collection name with special characters
	_, err := mapAndValidateCollectionName("test1234/1")
	assert.Error(t, err, "Error expected because the name contains illegal chars")

	//create a new instance and collection object using a collection name with special characters
	_, err = mapAndValidateCollectionName("5test1234")
	assert.Error(t, err, "Error expected because the name starts with a number")

	//create a new instance and collection object using an empty string
	_, err = mapAndValidateCollectionName("")
	assert.Error(t, err, "Error should have been thrown for an invalid name")

	_, err = mapAndValidateCollectionName("a12345678901234567890123456789012345678901234" +
		"56789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
		"12345678901234567890123456789012345678901234567890123456789012345678901234567890123456" +
		"78901234567890123456789012345678901234567890")
	assert.Error(t, err, "Error should have been thrown for an invalid name")

	transformedName, err := mapAndValidateCollectionName("test.my.db-1")
	assert.NoError(t, err, "")
	assert.Equal(t, "test_my_db-1", transformedName)
}

func TestConstructMetadataDBName(t *testing.T) {
	// Allowed pattern for chainName: [a-zA-Z0-9]+([-_][a-zA-Z0-9]+)
	chainName := "tob2g_y-z0f_qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v_1qmmqjzz0ox62k_507sh3fkw3-mfqh_ukfvxlm5szfbwtpfkd1r4j_cy8oft5obvwqpzjxb27xuw6"

	truncatedChainName := "tob2g_y-z0f_qwp"
	assert.Equal(t, chainNameAllowedLength, len(truncatedChainName))

	// <first 15 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName> + 1 char for ')' + 1 char for '_' = 82 chars
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(chainName)))
	expectedDBName := truncatedChainName + "(" + hash + ")" + "_"
	expectedDBNameLength := 82

	constructedDBName := ConstructMetadataCOLName(chainName)
	assert.Equal(t, expectedDBNameLength, len(constructedDBName))
	assert.Equal(t, expectedDBName, constructedDBName)
}

func TestConstructedNamespaceDBName(t *testing.T) {
	// === SCENARIO 1: chainName_ns$$coll ===
	db := "statedb"

	// Allowed pattern for chainName: [a-zA-Z0-9]+([-_][a-zA-Z0-9]+)
	chainName := "tob2g_y-z0f_qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v_1qmmqjzz0ox62k_507sh3fkw3-mfqh_ukfvxlm5szfbwtpfkd1r4j_cy8oft5obvwqpzjxb27xuw6"

	// Allowed pattern for namespace and collection: [a-zA-Z0-9_-]
	ns := "wMCnSXiV9YoIqNQyNvFVTdM8XnUtvrOFFIWsKelmP5NEszmNLl8YhtOKbFu3P_NgwgsYF8PsfwjYCD8f1XRpANQLoErDHwLlweryqXeJ6vzT2x0pS_GwSx0m6tBI0zOmHQOq_2De8A87x6zUOPwufC2T6dkidFxiuq8Sey2-5vUo_iNKCij3WTeCnKx78PUIg_U1gp4_0KTvYVtRBRvH0kz5usizBxPaiFu3TPhB9XLviScvdUVSbSYJ0Z"
	// first letter 'p' denotes private data namespace. We can use 'h' to denote hashed data namespace as defined in
	// privacyenabledstate/common_storage_db.go
	coll := "pvWjtfSTXVK8WJus5s6zWoMIciXd7qHRZIusF9SkOS6m8XuHCiJDE9cCRuVerq22Na8qBL2ywDGFpVMIuzfyEXLjeJb0mMuH4cwewT6r1INOTOSYwrikwOLlT_fl0V1L7IQEwUBB8WCvRqSdj6j5-E5aGul_pv_0UeCdwWiyA_GrZmP7ocLzfj2vP8btigrajqdH-irLO2ydEjQUAvf8fiuxru9la402KmKRy457GgI98UHoUdqV3f3FCdR"

	truncatedChainName := "tob2g_y-z0f_qwp"
	truncatedNs := "wMCnSXiV9YoIqNQ"
	truncatedColl := "pvWjtfSTXV"
	assert.Equal(t, chainNameAllowedLength, len(truncatedChainName))
	assert.Equal(t, namespaceNameAllowedLength, len(truncatedNs))
	assert.Equal(t, collectionNameAllowedLength, len(truncatedColl))

	untruncatedDBName := chainName + "_" + ns + "$$" + coll
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedDBName)))
	expectedDBName := truncatedChainName + "_" + truncatedNs + "_" + truncatedColl + "(" + hash + ")"
	// <first 15 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '_' + <first 15 chars
	// (i.e., namespaceNameAllowedLength) of namespace> + 1 chars for '_' + <first 10 chars
	// (i.e., collectionNameAllowedLength) of collection> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName_ns$$coll> + 1 char for ')' = 109 chars
	expectedDBNameLength := 108

	namespace := ns + "$$" + coll
	constructedDBName := ConstructNamespaceDBName(chainName, namespace, db)
	assert.Equal(t, expectedDBNameLength, len(constructedDBName))
	assert.Equal(t, expectedDBName, constructedDBName)

	// === SCENARIO 2: chainName_ns ===

	untruncatedDBName = chainName + "_" + ns
	hash = hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedDBName)))
	expectedDBName = truncatedChainName + "_" + truncatedNs + "(" + hash + ")"
	// <first 15 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '_' + <first 15 chars
	// (i.e., namespaceNameAllowedLength) of namespace> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName_ns> + 1 char for ')' = 97 chars
	expectedDBNameLength = 97

	namespace = ns
	constructedDBName = ConstructNamespaceDBName(chainName, namespace, db)
	assert.Equal(t, expectedDBNameLength, len(constructedDBName))
	assert.Equal(t, expectedDBName, constructedDBName)
}
