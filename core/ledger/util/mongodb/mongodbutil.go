///*
//Copyright IBM Corp. All Rights Reserved.
//SPDX-License-Identifier: Apache-2.0
//*/
//
package mongodb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/util"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

var expectedCollectionNamePattern = `[a-zA-Z][a-zA-Z0-9.$_()+-]*`
var maxLength = 120

// To restrict the length of mongoDB maximum to the
// allowed length of 120 chars, the string length limit
// for chain/channel name, namespace/chaincode name, and
// collection name, which constitutes the database name,
// is defined.
var chainNameAllowedLength = 15
var namespaceNameAllowedLength = 15
var collectionNameAllowedLength = 10

func IsJson(value []byte) bool {
	var result interface{}
	err := json.Unmarshal(value, &result)
	return err == nil
}

func Contains(src []string, value string) bool {

	if src == nil {
		return false
	}

	for _, v := range src {
		if v == value {
			return true
		}
	}

	return false
}

//mapAndValidateCollectionName checks to see if the database name contains illegal characters
//MongoDB Rules: Collection names should begin with an underscore or a letter character, and cannot:
//    contain the $.
//    be an empty string (e.g. "").
//    contain the null character.
//    begin with the system. prefix. (Reserved for internal use.)
//    Start with '.'.
//
//Restictions have already been applied to the database name from Orderer based on
//restrictions required by Kafka and monogoDB (except a '.' char). The databaseName
// passed in here is expected to follow `[a-zA-Z][a-zA-Z0-9._()+-]*` pattern.
//
//This validation will simply check whether the database name matches the above pattern and will replace
// all occurrence of '$, .' by '_'. This will not cause collisions in the transformed named
func mapAndValidateCollectionName(collectionName string) (string, error) {
	// test Length
	if len(collectionName) <= 0 {
		return "", errors.Errorf("collection name is illegal, cannot be empty")
	}
	if len(collectionName) > maxLength {
		return "", errors.Errorf("collection name is illegal, cannot be longer than %d", maxLength)
	}
	re, err := regexp.Compile(expectedCollectionNamePattern)
	if err != nil {
		return "", errors.Wrapf(err, "error compiling regexp: %s", expectedCollectionNamePattern)
	}
	matched := re.FindString(collectionName)
	if len(matched) != len(collectionName) {
		return "", errors.Errorf("Collection Name '%s' does not match pattern '%s'", collectionName, expectedCollectionNamePattern)
	}
	// replace all '$, .' to '_'. The databaseName passed in will never contain an '_'.
	// So, this translation will not cause collisions
	collectionName = strings.Replace(collectionName, ".", "_", -1)
	collectionName = strings.Replace(collectionName, "$", "_", -1)
	return collectionName, nil
}

//CreateMongoInstance creates a MongoDB instance
func CreateMongoInstance(mongoDBConnectURL, id, pw string, authSource string, databaseName string, maxRetries,
	maxRetriesOnStartup int, connectionTimeout time.Duration, metricsProvider metrics.Provider) (*MongoInstance, error) {

	logger.Debugf("Entering CreateMongoInstance()")
	mongoConf, err := CreateConnectionDefinition(mongoDBConnectURL,
		id, pw, authSource, databaseName, maxRetries, maxRetriesOnStartup, connectionTimeout)
	if err != nil {
		logger.Errorf("Error calling MongoDB CreateConnectionDefinition(): %s", err)
		return nil, err
	}

	//Create the MongoDB instance
	var client *mongo.Client

	if mongoConf.Username == "" || mongoConf.Password == "" {
		client, err = mongo.NewClient(options.Client().ApplyURI(mongoConf.URL))
	} else {
		client, err = mongo.NewClient(options.Client().SetAuth(
			options.Credential{Username: mongoConf.Username, Password: mongoConf.Password, AuthSource: mongoConf.AuthSource}).ApplyURI(mongoConf.URL))
	}
	mongoInstance := &MongoInstance{conf: mongoConf, client: client}
	mongoInstance.stats = newStats(metricsProvider)
	verifyErr := mongoInstance.VerifyMongoConfig()
	if verifyErr != nil {
		return nil, verifyErr
	}

	//check the MongoDB version number, return an error if the version is not at least 2.0.0
	errVersion := mongoInstance.checkMongoDBVersion(client)
	if errVersion != nil {
		return nil, errVersion
	}

	return mongoInstance, nil
}

//CreateMongoCollection creates a MongoDB Collection, as well as the underlying database if it does not exist
func CreateMongoCollection(mongoInstance *MongoInstance, colName string) (*MongoDatabase, error) {

	collectionName, err := mapAndValidateCollectionName(colName)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	// The database name is in the core.yaml configuration file.
	dbName := mongoInstance.conf.DatabaseName
	mongoDBDatabase := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: dbName, CollectionName: collectionName}

	return &mongoDBDatabase, nil
}

func ConstructMetadataCOLName(colName string) string {
	if len(colName) > maxLength {
		untruncatedCOLName := colName
		// Truncate the name if the length violates the allowed limit
		// As the passed colName is same as chain/channel name, truncate using chainNameAllowedLength
		colName = colName[:chainNameAllowedLength]
		// For metadataDB (i.e., chain/channel DB), the colName contains <first 15 chars
		// (i.e., chainNameAllowedLength) of chainName> + (SHA256 hash of actual chainName)
		colName = colName + "(" + hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedCOLName))) + ")"
		// 15 chars for dbName + 1 char for ( + 64 chars for sha256 + 1 char for ) = 81 chars
	}
	return colName + "_"
}

// ConstructNamespaceDBName truncates db name to mongodb allowed length to
func ConstructNamespaceDBName(chainName, namespace, databaseName string) string {
	var escapedNamespace string
	namespaceDBName := chainName + "_" + namespace

	// For namespaceDBName of form 'chainName_namespace', on length limit violation, the truncated
	// namespaceDBName would contain <first 15 chars (i.e., chainNameAllowedLength) of chainName> + "_" +
	// <first 15 chars (i.e., namespaceNameAllowedLength) chars of namespace> +
	// (<SHA256 hash of [chainName_namespace]>)
	//
	// For namespaceDBName of form 'chainName_namespace_collection', on length limit violation, the truncated
	// namespaceDBName would contain <first 15 chars (i.e., chainNameAllowedLength) of chainName> + "_" +
	// <first 15 chars (i.e., namespaceNameAllowedLength) of namespace> + "_" + <first 10 chars
	// (i.e., collectionNameAllowedLength) of collection> + (<SHA256 hash of [chainName_namespace_pcollection]>)

	if len(namespaceDBName) > (maxLength - len(databaseName)) {
		// Compute the hash of untruncated namespaceDBName that needs to be appended to
		// truncated namespaceDBName for maintaining uniqueness
		hashOfNamespaceDBName := hex.EncodeToString(util.ComputeSHA256([]byte(chainName + "_" + namespace)))

		// As truncated namespaceDBName is of form 'chainName_escapedNamespace', both chainName
		// and escapedNamespace need to be truncated to defined allowed length.
		if len(chainName) > chainNameAllowedLength {
			// Truncate chainName to chainNameAllowedLength
			chainName = chainName[0:chainNameAllowedLength]
		}
		// As escapedNamespace can be of either 'namespace' or 'namespace$$collectionName',
		// both 'namespace' and 'collectionName' need to be truncated to defined allowed length.
		// '$$' is used as joiner between namespace and collection name.
		// Split the escapedNamespace into escaped namespace and escaped collection name if exist.
		names := strings.Split(namespace, "$$")
		namespace := names[0]
		if len(namespace) > namespaceNameAllowedLength {
			// Truncate the namespace
			namespace = namespace[0:namespaceNameAllowedLength]
		}
		escapedNamespace = namespace

		// Check and truncate the length of collection name if exist
		if len(names) == 2 {
			collection := names[1]
			if len(collection) > collectionNameAllowedLength {
				// Truncate the escaped collection name
				collection = collection[0:collectionNameAllowedLength]
			}
			// Append truncated collection name to escapedNamespace
			escapedNamespace = escapedNamespace + "_" + collection

		}
		// Construct and return the namespaceDBName
		// 15 chars for chainName + 1 char for '_' + 26 chars for escaped namespace + 1 char for '(' + 64 chars
		// for sha256 hash + 1 char for ')' = 108 chars
		return chainName + "_" + escapedNamespace + "(" + hashOfNamespaceDBName + ")"
	} else {
		namespaceDBName = strings.Replace(namespaceDBName, "$$", "_", -1)
	}

	return namespaceDBName
}

//checkMongoDBVersion verifies MongoDB is at least 3.4.x
func (mongoInstance *MongoInstance) checkMongoDBVersion(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leastSupportedVersionFloat := 3.4

	mongoInstance.client.Connect(ctx)

	err := mongoInstance.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB [%s]", err)
	}

	var serverStatus bsonx.Doc

	mongoInstance.client.Database("admin").RunCommand(context.Background(),
		bsonx.Doc{bsonx.Elem{Key: "serverStatus", Value: bsonx.Int32(1)}}).Decode(&serverStatus)

	version, _ := serverStatus.LookupErr("version")
	//split the version into parts
	majorVersion := strings.Split(version.String(), ".")
	majorVersionFloat, _ := strconv.ParseFloat(majorVersion[0]+"."+majorVersion[1], 64)
	//check to see that the major version number is at least 3.4.x
	if majorVersionFloat < leastSupportedVersionFloat {
		return errors.Errorf("MongoDB must be at least version 3.4.x. Detected version %s", version)
	}

	return nil
}
