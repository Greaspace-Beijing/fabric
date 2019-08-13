package mongodb

import (
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const badConnectURL = "mongodb:2701"
const badParseConnectURL = "mongodb://host.com|2701"

var mongoDBDef *MongoDBDef

func cleanupCollection(collection string) error {
	//create a new connection
	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	if err != nil {
		return err
	}
	db := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: mongoInstance.conf.DatabaseName, CollectionName: collection}
	//drop the test collection
	db.DropCollection()
	if err != nil {
		return err
	}
	return nil
}

var assetJSON = []byte(`{"asset_name":"marble1","color":"blue","size": "35","owner":"jerry"}`)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	// Read the core.yaml file for default config.
	ledgertestutil.SetupCoreYAMLConfig()

	// Switch to MongoDB
	mongoAddress, cleanup := mongoDBSetup()
	defer cleanup()

	viper.Set("ledger.state.stateDatabase", "MongoDB")
	defer viper.Set("ledger.state.stateDatabase", "goleveldb")

	viper.Set("ledger.state.mongoDBConfig.mongoDBAddress", mongoAddress)
	// Replace with correct username/password such as
	// admin/admin if user security is enabled on mongodb.
	viper.Set("ledger.state.mongoDBConfig.username", "")
	viper.Set("ledger.state.mongoDBConfig.password", "")
	viper.Set("ledger.state.mongoDBConfig.maxRetries", 3)
	viper.Set("ledger.state.mongoDBConfig.maxRetriesOnStartup", 12)
	viper.Set("ledger.state.mongoDBConfig.requestTimeout", time.Second*35)

	//set the logging level to DEBUG to test debug only code
	flogging.ActivateSpec("mongodb=debug")

	// Create MongoDB definition from config parameters
	mongoDBDef = GetMongoDBDefinition()

	return m.Run()
}

func mongoDBSetup() (addr string, cleanup func()) {
	externalMongo, set := os.LookupEnv("MONGODB_ADDR")
	if set {
		return externalMongo, func() {}
	}

	mongoDB := &runner.MongoDB{}
	if err := mongoDB.Start(); err != nil {
		err := fmt.Errorf("failed to start mongoDB: %s", err)
		panic(err)
	}
	return mongoDB.Address(), func() { mongoDB.Stop() }
}

func TestDBCreateSaveWithoutRevision(t *testing.T) {

	collection := "testdbcreatesavewithoutrevision"
	err := cleanupCollection(collection)
	assert.NoError(t, err, "Error when trying to cleanup  Error: %s", err)
	defer cleanupCollection(collection)

	//create a new instance and database object
	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	assert.NoError(t, err, "Error when trying to create mongo instance")
	db := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: mongoInstance.conf.DatabaseName, CollectionName: collection}

	//Save the test document
	_, saveerr := db.SaveDoc("2", "", &MongoDoc{JSONValue: assetJSON, BinaryDatas: nil})
	assert.NoError(t, saveerr, "Error when trying to save a document")

}

func TestDBConnectionDef(t *testing.T) {

	//create a new connection
	_, err := CreateConnectionDefinition(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout)
	assert.NoError(t, err, "Error when trying to create database connection definition")

}

func TestDBBadConnectionDef(t *testing.T) {

	//create a new connection
	_, err := CreateConnectionDefinition(badParseConnectURL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout)
	assert.Error(t, err, "Did not receive error when trying to create database connection definition with a bad hostname")

}

func TestEncodePathElement(t *testing.T) {

	encodedString := encodePathElement("testelement")
	assert.Equal(t, "testelement", encodedString)

	encodedString = encodePathElement("test element")
	assert.Equal(t, "test%20element", encodedString)

	encodedString = encodePathElement("/test element")
	assert.Equal(t, "%2Ftest%20element", encodedString)

	encodedString = encodePathElement("/test element:")
	assert.Equal(t, "%2Ftest%20element:", encodedString)

	encodedString = encodePathElement("/test+ element:")
	assert.Equal(t, "%2Ftest%2B%20element:", encodedString)

}

func TestHealthCheck(t *testing.T) {

	//Create a bad mongodb instance
	badConnectDef, _ := CreateConnectionDefinition(badConnectURL, "", "", "", "", 1, 1, time.Second*30)
	client, _ := badConnectDef.CreateConnectionClient()
	badMongoDBInstance := MongoInstance{badConnectDef, client, newStats(&disabled.Provider{})}

	err := badMongoDBInstance.HealthCheck()
	assert.Error(t, err, "Health check should result in an error if unable to connect to MongoDB")
	assert.Contains(t, err.Error(), "failed to connect to MongoDB")

	//Create a good mongodb instance
	goodConnectDef, _ := CreateConnectionDefinition(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout)
	client, _ = goodConnectDef.CreateConnectionClient()
	goodMongoDBInstance := MongoInstance{goodConnectDef, client, newStats(&disabled.Provider{})}

	err = goodMongoDBInstance.HealthCheck()
	assert.NoError(t, err)
}

func TestDBSaveBinaryData(t *testing.T) {

	collection := "testdbsavebinarydata"
	err := cleanupCollection(collection)
	assert.NoError(t, err, "Error when trying to cleanup  Error: %s", err)
	defer cleanupCollection(collection)

	byteText := []byte(`This is a test document.  This is only a test`)

	binaryData := &BinaryDataInfo{}
	binaryData.BinaryData = byteText
	binaryData.Length = uint64(len(byteText))
	binaryData.Name = "valueBytes"

	binaryDatas := []*BinaryDataInfo{}
	binaryDatas = append(binaryDatas, binaryData)

	//create a new instance and database object
	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	assert.NoError(t, err, "Error when trying to create mongo instance")
	db := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: mongoInstance.conf.DatabaseName, CollectionName: collection}

	//Save the test document
	//_, saveerr := db.SaveDoc("10", "", &MongoDoc{JSONValue: byteText})
	_, saveerr := db.SaveDoc("10", "", &MongoDoc{JSONValue: assetJSON, BinaryDatas: binaryDatas})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Attempt to retrieve the updated test document with binaryDatas
	mongoDoc, _, geterr2 := db.ReadDoc("10")
	assert.NoError(t, geterr2, "Error when trying to retrieve a document with binaryData")
	assert.NotNil(t, mongoDoc.BinaryDatas)
	assert.Equal(t, byteText, mongoDoc.BinaryDatas[0].BinaryData)
	assert.Equal(t, binaryData.Length, mongoDoc.BinaryDatas[0].Length)

}

func TestIndexOperations(t *testing.T) {

	collection := "testindexoperations"
	err := cleanupCollection(collection)
	assert.NoError(t, err, "Error when trying to cleanupCollection  Error: %s", err)
	defer cleanupCollection(collection)

	byteJSON1 := []byte(`{"_id":"1", "asset_name":"marble1","color":"blue","size":1,"owner":"jerry"}`)
	byteJSON2 := []byte(`{"_id":"2", "asset_name":"marble2","color":"red","size":2,"owner":"tom"}`)
	byteJSON3 := []byte(`{"_id":"3", "asset_name":"marble3","color":"green","size":3,"owner":"jerry"}`)
	byteJSON4 := []byte(`{"_id":"4", "asset_name":"marble4","color":"purple","size":4,"owner":"tom"}`)
	byteJSON5 := []byte(`{"_id":"5", "asset_name":"marble5","color":"blue","size":5,"owner":"jerry"}`)
	byteJSON6 := []byte(`{"_id":"6", "asset_name":"marble6","color":"white","size":6,"owner":"tom"}`)
	byteJSON7 := []byte(`{"_id":"7", "asset_name":"marble7","color":"white","size":7,"owner":"tom"}`)
	byteJSON8 := []byte(`{"_id":"8", "asset_name":"marble8","color":"white","size":8,"owner":"tom"}`)
	byteJSON9 := []byte(`{"_id":"9", "asset_name":"marble9","color":"white","size":9,"owner":"tom"}`)
	byteJSON10 := []byte(`{"_id":"10", "asset_name":"marble10","color":"white","size":10,"owner":"tom"}`)

	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	assert.NoError(t, err, "Error when trying to create mongo instance")
	db := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: mongoInstance.conf.DatabaseName, CollectionName: collection}

	batchUpdateDocs := []*MongoDoc{}
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON1, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON2, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON3, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON4, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON5, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON6, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON7, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON8, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON9, BinaryDatas: nil})
	batchUpdateDocs = append(batchUpdateDocs, &MongoDoc{JSONValue: byteJSON10, BinaryDatas: nil})

	_, err = db.BatchUpdateDocuments(batchUpdateDocs)
	assert.NoError(t, err, "Error adding batch of documents")

	//Create an index definition. -1 is descending
	indexDefSize := `{"index":{"fields":[{"size":-1}]}, "name":"indexSizeSortName"}`

	//Create the index
	_, err = db.CreateIndex(indexDefSize)
	assert.NoError(t, err, "Error thrown while creating an index")

	//Retrieve the list of indexes
	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)
	listResult, err := db.ListIndex()
	assert.NoError(t, err, "Error thrown while retrieving indexes")

	//There should only be one item returned
	assert.Equal(t, 1, len(listResult))

	//Verify the returned definition
	for _, elem := range listResult {
		assert.Equal(t, "indexSizeSortName", elem.Name)
		assert.Equal(t, elem.Key, map[string]interface{}{"size": int32(-1)})
	}

	//Create an index definition with no DesignDocument or name. -1 is descending
	indexDefColor := `{"index":{"fields":[{"color":-1}]}}`

	//Create the index
	_, err = db.CreateIndex(indexDefColor)
	assert.NoError(t, err, "Error thrown while creating an index")

	//Retrieve the list of indexes
	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)
	listResult, err = db.ListIndex()
	assert.NoError(t, err, "Error thrown while retrieving indexes")

	//There should be two indexes returned
	assert.Equal(t, 2, len(listResult))

	//Delete the named index
	err = db.DropIndex("indexSizeSortName")
	assert.NoError(t, err, "Error thrown while deleting an index")

	//Retrieve the list of indexes
	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)
	listResult, err = db.ListIndex()
	assert.NoError(t, err, "Error thrown while retrieving indexes")

	//There should be one index returned
	assert.Equal(t, 1, len(listResult))

	//Delete the unnamed index
	for _, elem := range listResult {
		err = db.DropIndex(elem.Name)
		assert.NoError(t, err, "Error thrown while deleting an index")
	}

	//Retrieve the list of indexes
	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)
	listResult, err = db.ListIndex()
	assert.NoError(t, err, "Error thrown while retrieving indexes")
	assert.Equal(t, 0, len(listResult))

	//Create a query string with a descending sort, this will require an index
	queryString := `{"query":{"size": {"$gt": 0}}, "projection":{"_id":true, "_rev":true, "owner":true, "asset_name":true, "color":true, "size":true}, "sort":{"size":-1}, "limit": 10,"skip": "0"}`

	//Execute a query with a sort, this should throw the exception
	_, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error should have thrown while querying without a valid index")

	//Create the index
	_, err = db.CreateIndex(indexDefSize)
	assert.NoError(t, err, "Error thrown while creating an index")

	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)

	//Execute a query with an index,  this should succeed
	_, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error thrown while querying with an index")

	//Create another index definition
	indexDefSize = `{"index":{"fields":[{"data.size":-1}, {"data.owner":-1}]}, "name":"indexSizeOwnerSortName"}`
	//Create the index
	dbResp, err := db.CreateIndex(indexDefSize)
	assert.NoError(t, err, "Error thrown while creating an index")

	//verify the response is "created" for an index creation
	assert.Equal(t, "created", dbResp)

	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)

	//Update the index
	dbResp, err = db.CreateIndex(indexDefSize)
	assert.NoError(t, err, "Error thrown while creating an index")
	//verify the response is "exists" for an update

	//Retrieve the list of indexes
	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)
	listResult, err = db.ListIndex()
	assert.NoError(t, err, "Error thrown while retrieving indexes")

	//There should only be two definitions
	assert.Equal(t, 2, len(listResult))

	//Create an invalid index definition with an invalid JSON
	indexDefSize = `{"index:"{"fields":[{"data.size":-1}, {"data.owner":-1}]}, "name":"indexSizeOwnerSortName"}`

	//Create the index
	_, err = db.CreateIndex(indexDefSize)
	assert.Error(t, err, "Error should have been thrown for an invalid index JSON")

	//Create an invalid index definition with a valid JSON and an invalid index definition
	indexDefSize = `{"index":{"fields2":[{"data.size":-1}, {"data.owner":-1}]}, "name":"indexSizeOwnerSortName"}`

	//Create the index
	_, err = db.CreateIndex(indexDefSize)
	assert.Error(t, err, "Error should have been thrown for an invalid index definition")

}

func TestRichQuery(t *testing.T) {

	byteJSON01 := []byte(`{"asset_name":"marble01","color":"blue","size":1,"owner":"jerry"}`)
	byteJSON02 := []byte(`{"asset_name":"marble02","color":"red","size":2,"owner":"tom"}`)
	byteJSON03 := []byte(`{"asset_name":"marble03","color":"green","size":3,"owner":"jerry"}`)
	byteJSON04 := []byte(`{"asset_name":"marble04","color":"purple","size":4,"owner":"tom"}`)
	byteJSON05 := []byte(`{"asset_name":"marble05","color":"blue","size":5,"owner":"jerry"}`)
	byteJSON06 := []byte(`{"asset_name":"marble06","color":"white","size":6,"owner":"tom"}`)
	byteJSON07 := []byte(`{"asset_name":"marble07","color":"white","size":7,"owner":"tom"}`)
	byteJSON08 := []byte(`{"asset_name":"marble08","color":"white","size":8,"owner":"tom"}`)
	byteJSON09 := []byte(`{"asset_name":"marble09","color":"white","size":9,"owner":"tom"}`)
	byteJSON10 := []byte(`{"asset_name":"marble10","color":"white","size":10,"owner":"tom"}`)
	byteJSON11 := []byte(`{"asset_name":"marble11","color":"green","size":11,"owner":"tom"}`)
	byteJSON12 := []byte(`{"asset_name":"marble12","color":"green","size":12,"owner":"frank"}`)

	binaryData1 := &BinaryDataInfo{}
	binaryData1.BinaryData = []byte(`marble01 - test binaryData`)
	binaryData1.Name = "data"
	binaryDatas1 := []*BinaryDataInfo{}
	binaryDatas1 = append(binaryDatas1, binaryData1)

	binaryData2 := &BinaryDataInfo{}
	binaryData2.BinaryData = []byte(`marble02 - test binaryData`)
	binaryData2.Name = "data"
	binaryDatas2 := []*BinaryDataInfo{}
	binaryDatas2 = append(binaryDatas2, binaryData2)

	binaryData3 := &BinaryDataInfo{}
	binaryData3.BinaryData = []byte(`marble03 - test binaryData`)
	binaryData3.Name = "data"
	binaryDatas3 := []*BinaryDataInfo{}
	binaryDatas3 = append(binaryDatas3, binaryData3)

	binaryData4 := &BinaryDataInfo{}
	binaryData4.BinaryData = []byte(`marble04 - test binaryData`)
	binaryData4.Name = "data"
	binaryDatas4 := []*BinaryDataInfo{}
	binaryDatas4 = append(binaryDatas4, binaryData4)

	binaryData5 := &BinaryDataInfo{}
	binaryData5.BinaryData = []byte(`marble05 - test binaryData`)
	binaryData5.Name = "data"
	binaryDatas5 := []*BinaryDataInfo{}
	binaryDatas5 = append(binaryDatas5, binaryData5)

	binaryData6 := &BinaryDataInfo{}
	binaryData6.BinaryData = []byte(`marble06 - test binaryData`)
	binaryData6.Name = "data"
	binaryDatas6 := []*BinaryDataInfo{}
	binaryDatas6 = append(binaryDatas6, binaryData6)

	binaryData7 := &BinaryDataInfo{}
	binaryData7.BinaryData = []byte(`marble07 - test binaryData`)
	binaryData7.Name = "data"
	binaryDatas7 := []*BinaryDataInfo{}
	binaryDatas7 = append(binaryDatas7, binaryData7)

	binaryData8 := &BinaryDataInfo{}
	binaryData8.BinaryData = []byte(`marble08 - test binaryData`)
	binaryData8.Name = "data"
	binaryDatas8 := []*BinaryDataInfo{}
	binaryDatas8 = append(binaryDatas8, binaryData8)

	binaryData9 := &BinaryDataInfo{}
	binaryData9.BinaryData = []byte(`marble09 - test binaryData`)
	binaryData9.Name = "data"
	binaryDatas9 := []*BinaryDataInfo{}
	binaryDatas9 = append(binaryDatas9, binaryData9)

	binaryData10 := &BinaryDataInfo{}
	binaryData10.BinaryData = []byte(`marble10 - test binaryData`)
	binaryData10.Name = "data"
	binaryDatas10 := []*BinaryDataInfo{}
	binaryDatas10 = append(binaryDatas10, binaryData10)

	binaryData11 := &BinaryDataInfo{}
	binaryData11.BinaryData = []byte(`marble11 - test binaryData`)
	binaryData11.Name = "data"
	binaryDatas11 := []*BinaryDataInfo{}
	binaryDatas11 = append(binaryDatas11, binaryData11)

	binaryData12 := &BinaryDataInfo{}
	binaryData12.BinaryData = []byte(`marble12 - test binaryData`)
	binaryData12.Name = "data"
	binaryDatas12 := []*BinaryDataInfo{}
	binaryDatas12 = append(binaryDatas12, binaryData12)

	collection := "testrichquery"
	err := cleanupCollection(collection)
	assert.NoError(t, err, "Error when trying to cleanup  Error: %s", err)
	//defer cleanupCollection(collection)

	//create a new instance and database object   --------------------------------------------------------
	mongoInstance, err := CreateMongoInstance(mongoDBDef.URL, mongoDBDef.Username, mongoDBDef.Password, mongoDBDef.AuthSource,
		mongoDBDef.DatabaseName, mongoDBDef.MaxRetries, mongoDBDef.MaxRetriesOnStartup, mongoDBDef.RequestTimeout, &disabled.Provider{})
	assert.NoError(t, err, "Error when trying to create mongo instance")
	db := MongoDatabase{MongoInstance: mongoInstance, DatabaseName: mongoInstance.conf.DatabaseName, CollectionName: collection}

	//Save the test document
	_, saveerr := db.SaveDoc("marble01", "", &MongoDoc{JSONValue: byteJSON01, BinaryDatas: binaryDatas1})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble02", "", &MongoDoc{JSONValue: byteJSON02, BinaryDatas: binaryDatas2})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble03", "", &MongoDoc{JSONValue: byteJSON03, BinaryDatas: binaryDatas3})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble04", "", &MongoDoc{JSONValue: byteJSON04, BinaryDatas: binaryDatas4})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble05", "", &MongoDoc{JSONValue: byteJSON05, BinaryDatas: binaryDatas5})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble06", "", &MongoDoc{JSONValue: byteJSON06, BinaryDatas: binaryDatas6})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble07", "", &MongoDoc{JSONValue: byteJSON07, BinaryDatas: binaryDatas7})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble08", "", &MongoDoc{JSONValue: byteJSON08, BinaryDatas: binaryDatas8})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble09", "", &MongoDoc{JSONValue: byteJSON09, BinaryDatas: binaryDatas9})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble10", "", &MongoDoc{JSONValue: byteJSON10, BinaryDatas: binaryDatas10})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble11", "", &MongoDoc{JSONValue: byteJSON11, BinaryDatas: binaryDatas11})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Save the test document
	_, saveerr = db.SaveDoc("marble12", "", &MongoDoc{JSONValue: byteJSON12, BinaryDatas: binaryDatas12})
	assert.NoError(t, saveerr, "Error when trying to save a document")

	//Test query with invalid JSON -------------------------------------------------------------------
	queryString := `{"query":{"owner":}}`

	_, _, err = db.QueryDocuments(queryString)
	assert.Error(t, err, "Error should have been thrown for bad json")

	//Test query with object  -------------------------------------------------------------------
	queryString = `{"query":{"owner":{"$eq":"jerry"}}}`

	queryResult, _, err := db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 3 results for owner="jerry"
	assert.Equal(t, 3, len(queryResult))

	//Test query with implicit operator   --------------------------------------------------------------
	queryString = `{"query":{"owner":"jerry"}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 3 results for owner="jerry"
	assert.Equal(t, 3, len(queryResult))

	//Test query with specified fields   -------------------------------------------------------------------
	queryString = `{"query":{"owner":{"$eq":"jerry"}},"fields": ["owner","asset_name","color","size"]}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 3 results for owner="jerry"
	assert.Equal(t, 3, len(queryResult))

	//Test query with a leading operator   -------------------------------------------------------------------
	queryString = `{"query":{"$or":[{"owner":{"$eq":"jerry"}},{"owner": {"$eq": "frank"}}]}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 4 results for owner="jerry" or owner="frank"
	assert.Equal(t, 4, len(queryResult))

	//Test query implicit and explicit operator   ------------------------------------------------------------------
	queryString = `{"query":{"color":"green","$or":[{"owner":"tom"},{"owner":"frank"}]}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 2 results for color="green" and (owner="jerry" or owner="frank")
	assert.Equal(t, 2, len(queryResult))

	//Test query with a leading operator  -------------------------------------------------------------------------
	queryString = `{"query":{"$and":[{"size":{"$gte":2}},{"size":{"$lte":5}}]}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 4 results for size >= 2 and size <= 5
	assert.Equal(t, 4, len(queryResult))

	//Test query with leading and embedded operator  -------------------------------------------------------------
	queryString = `{"query":{"$and":[{"size":{"$gte":3}},{"size":{"$lte":10}},{"size":{"$not":{"$eq":7}}}]}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 7 results for size >= 3 and size <= 10 and not 7
	assert.Equal(t, 7, len(queryResult))

	//Test query with query operator and array of objects ----------------------------------------------------------
	queryString = `{"query":{"$and":[{"size":{"$gte":2}},{"size":{"$lte":10}},{"$nor":[{"size":3},{"size":5},{"size":7}]}]}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 6 results for size >= 2 and size <= 10 and not 3,5 or 7
	assert.Equal(t, 6, len(queryResult))

	//Test a range query ---------------------------------------------------------------------------------------------
	queryResult, _, err = db.ReadDocRange("marble02", "marble06", 10000)
	assert.NoError(t, err, "Error when attempting to execute a range query")

	//There should be 4 results
	assert.Equal(t, 4, len(queryResult))

	//BinaryDatas retrieved should be correct
	assert.Equal(t, binaryData2.BinaryData, queryResult[0].BinaryDatas[0].BinaryData)
	assert.Equal(t, binaryData3.BinaryData, queryResult[1].BinaryDatas[0].BinaryData)
	assert.Equal(t, binaryData4.BinaryData, queryResult[2].BinaryDatas[0].BinaryData)
	assert.Equal(t, binaryData5.BinaryData, queryResult[3].BinaryDatas[0].BinaryData)

	//Test query with for tom  -------------------------------------------------------------------
	queryString = `{"query":{"owner":{"$eq":"tom"}}}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 8 results for owner="tom"
	assert.Equal(t, 8, len(queryResult))

	//Test query with for tom with limit  -------------------------------------------------------------------
	queryString = `{"query":{"owner":{"$eq":"tom"}},"limit":2}`

	queryResult, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query")

	//There should be 2 results for owner="tom" with a limit of 2
	assert.Equal(t, 2, len(queryResult))

	//Create an index definition
	indexDefSize := `{"index":{"fields":[{"size":-1}, {"asset_name":-1}]}, "name":"indexSizeSortName"}`

	//Create the index
	_, err = db.CreateIndex(indexDefSize)
	assert.NoError(t, err, "Error thrown while creating an index")

	//Delay for 100ms since MongoDB index list is updated async after index create/drop
	time.Sleep(100 * time.Millisecond)

	//Test query with valid index  -------------------------------------------------------------------
	queryString = `{"query":{"size":{"$gt":0}}, "hint": {"size":-1, "asset_name":-1}}`

	_, _, err = db.QueryDocuments(queryString)
	assert.NoError(t, err, "Error when attempting to execute a query with a valid index")

}
