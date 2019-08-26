/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mongodb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap/zapcore"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var logger = flogging.MustGetLogger("mongodb")

//time between retry attempts in milliseconds
const retryWaitTime = 125

const (
	binaryField  = "_binaryData"
	idField      = "_id"
	revField     = "_rev"
	versionField = "~version"
	deletedField = "_deleted"
)

// DBOperationResponse is body for successful database calls.
type DBOperationResponse struct {
	Ok  bool
	id  string
	rev string
}

//MongoDoc defines the structure for a JSON document value
type MongoDoc struct {
	JSONValue   []byte
	BinaryDatas []*BinaryDataInfo
}

type BinaryDataInfo struct {
	Name       string
	Length     uint64
	BinaryData []byte
}

//MongoConnectionDef contains parameters
type MongoConnectionDef struct {
	URL                 string
	Username            string
	Password            string
	AuthSource          string
	DatabaseName        string
	MaxRetries          int
	MaxRetriesOnStartup int
	RequestTimeout      time.Duration
}

//MongoInstance represents a MongoDB instance
type MongoInstance struct {
	conf   *MongoConnectionDef //connection configuration
	client *mongo.Client       // a client to connect to this instance
	stats  *stats
}

//MongoDatabase represents a database within a MongoDB instance
type MongoDatabase struct {
	MongoInstance  *MongoInstance //connection configuration
	DatabaseName   string
	CollectionName string
}

// DBInfo is body for database information.
type DBInfo struct {
	Db          string  `bson:"db"`
	Collections int     `bson:"collections"`
	Views       int     `bson:"views"`
	Objects     int     `bson:"objects"`
	AvgObjSize  float64 `bson:"avgObjSize"`
	DataSize    float64 `bson:"dataSize"`
	StorageSize float64 `bson:"storageSize"`
	NumExtents  int     `bson:"numExtents"`
	Indexes     int     `bson:"indexes"`
	IndexSize   float64 `bson:"indexSize"`
	FsUsedSize  float64 `bson:"fsUsedSize"`
	FsTotalSize float64 `bson:"fsTotalSize"`
	Ok          string  `bson:"ok"`
}

// DocMetadata is used for capturing MongoDB document header info,
// used to capture id, version, rev and binarydata returned in the query from MongoDB
type DocMetadata struct {
	ID          string            `bson:"_id"`
	Rev         int               `bson:"_rev"`
	Version     string            `bson:"~version"`
	BinaryDatas []*BinaryDataInfo `bson:"_binaryData"`
}

//BatchUpdateResponse defines a structure for batch update response
type BatchUpdateResponse struct {
	ID     string `bson:"_id"`
	Rev    int    `bson:"_rev"`
	Error  string
	Reason string
}

type QueryResult struct {
	ID          string `bson:"_id"`
	Value       []byte
	BinaryDatas []*BinaryDataInfo `bson:"_binaryData"`
}

type RangeQueryResponse struct {
	TotalRows int64
	Offset    int
	Rows      []*QueryResult
}

type ChainCodeMongoIndex struct {
	Index  map[string]interface{} `json:"index"`
	Name   string                 `json:"name"`
	Unique bool                   `json:"unique"`
}

type MongoIndex struct {
	//Index  map[string][]string `json:"index"`
	Key    map[string]interface{} `json:"key"`
	Name   string                 `json:"name"`
	Unique bool                   `json:"unique"`
}

type MongoQuery struct {
	PagingInfo *PagingInfo `json:"pagingInfo"`
	Query      interface{} `json:"query"`
	Projection interface{} `json:"projection"`
	Sort       interface{} `json:"sort"`
	Limit      int         `json:"limit"`
	Skip       string      `json:"skip"`
	Hint       interface{} `json:"hint"`
}

//Paging info used for paging query
//The result of query will not change with same query conditions(even updated data)
//TotalCount: the total count of query result
//TotalPage: the amount of page of query result
//LastPageRecordCount: the amount of record of the last page of query result
//CurrentPageNum: the current page number you want to query
//PageSize: the size of a page contains
//LastQueryPageNum: the page number of last time query
//LastQueryObjectId: the objectid of the record at last of result of last time query
//LastRecordObjectId: the objectid of the record of the query
//SortBy: the item sorted by(set to _id and will never change)
type PagingInfo struct {
	TotalCount          int                `json:"totalCount"`
	TotalPage           int                `json:"totalPage"`
	LastPageRecordCount int                `json:"lastPageRecordCount"`
	CurrentPageNum      int                `json:"currentPageNum"`
	PageSize            int                `json:"pageSize"`
	LastQueryPageNum    int                `json:"lastQueryPageNum"`
	LastQueryObjectId   primitive.ObjectID `json:"lastQueryObjectId"`
	LastRecordObjectId  primitive.ObjectID `json:"lastRecordObjectId"`
	SortBy              string             `json:"sortBy"`
}

//GetDatabaseInfo method provides function to retrieve database information
func (dbclient *MongoDatabase) GetDatabaseInfo() (*DBInfo, error) {
	client := dbclient.MongoInstance.client
	colName := dbclient.CollectionName
	defer dbclient.MongoInstance.recordMetric(time.Now(), colName, "GetDatabaseInfo")
	dbStats := &DBInfo{}
	err := client.Database(dbclient.DatabaseName).RunCommand(context.Background(),
		bson.D{{"dbStats", 1},
			{"scale", 1024 * 1000}}).Decode(&dbStats)

	if err != nil {
		return nil, err
	}

	// trace the database info response
	logger.Debugw("GetDatabaseInfo()", "dbStats", dbStats)

	return dbStats, nil

}

// HealthCheck checks if the peer is able to communicate with MongoDB
func (mongoInstance *MongoInstance) HealthCheck() error {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	_ = mongoInstance.client.Connect(ctx)
	err := mongoInstance.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB [%s]", err.Error())
	}
	return nil
}

//DropDatabase provides method to drop an existing database
func (dbclient *MongoDatabase) DropDatabase() error {
	dbName := dbclient.DatabaseName

	logger.Debugf("Database Name : [%s] Entering DropDatabase()", dbName)

	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	err := client.Database(dbName).Drop(ctx)

	if err != nil {
		return err
	} else {
		logger.Debugf("[%s] Dropped database", dbclient.DatabaseName)
	}

	logger.Debugf("Database Name : [%s] Exiting DropDatabase()", dbclient.DatabaseName)

	return nil

}

//DropCollection provides method to drop an existing collection
func (dbclient *MongoDatabase) DropCollection() error {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering DropCollection()", dbName, colName)

	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	err := client.Database(dbName).Collection(colName).Drop(ctx)

	if err != nil {
		return err
	} else {
		logger.Debugf("[%s] Dropped collection", colName)
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting DropCollection()", dbName, colName)

	return nil

}

//ReadDoc method provides function to retrieve a document and its revision
//from the database by id
func (dbclient *MongoDatabase) ReadDoc(id string) (*MongoDoc, string, error) {
	var mongoDoc MongoDoc
	var docMetadata *DocMetadata
	var revision string

	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	defer dbclient.MongoInstance.recordMetric(time.Now(), colName, "ReadDoc")
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)
	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering ReadDoc()  id=[%s]", dbName, colName, id)
	if !utf8.ValidString(id) {
		return nil, "", errors.Errorf("doc id [%x] not a valid utf8 string", id)
	}

	res := client.Database(dbName).Collection(colName).FindOne(ctx, bson.M{"_id": id})
	if res.Err() != nil {
		return nil, "", res.Err()
	}

	err := res.Decode(&docMetadata)
	if err != nil {
		if strings.Contains(err.Error(), "no documents in result") {
			logger.Debugf("Database Name : [%s] Collection Name : [%s] Document not found", dbclient.DatabaseName, dbclient.CollectionName)
			return nil, "", nil
		}
		return nil, "", err
	}

	//Get the revision from header
	revision = strconv.Itoa(docMetadata.Rev)
	if revision == "" {
		return nil, "", res.Err()
	}

	if docMetadata.BinaryDatas != nil {
		for _, binaryDataInfo := range docMetadata.BinaryDatas {
			name := binaryDataInfo.Name
			length := binaryDataInfo.Length
			binaryData := binaryDataInfo.BinaryData
			binaryDataInfo := BinaryDataInfo{Name: name, Length: length, BinaryData: binaryData}
			mongoDoc.BinaryDatas = append(mongoDoc.BinaryDatas, &binaryDataInfo)
		}
	}

	var jsonValue = make(map[string]interface{})
	jsonRaw, _ := res.DecodeBytes()
	elem, _ := jsonRaw.Elements()
	for _, value := range elem {
		key := value.Key()
		if key == idField || key == revField || key == versionField || key == binaryField {
			continue
		}

		if value.Value().IsNumber() {
			if _, ok := value.Value().Int64OK(); ok {
				jsonValue[key] = value.Value().Int64()
			}
			if _, ok := value.Value().Int32OK(); ok {
				jsonValue[key] = int64(value.Value().Int32())
			}
		} else {
			jsonValue[key] = value.Value().String()
		}
	}

	jsonValue[idField] = docMetadata.ID
	jsonValue[revField] = docMetadata.Rev
	versionRune := []rune(docMetadata.Version)
	jsonValue[versionField] = string(versionRune)

	data, err := json.Marshal(jsonValue)
	if err != nil {
		return nil, "", errors.Wrap(err, "error marshalling json data")
	}
	mongoDoc.JSONValue = data

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting ReadDoc()", dbName, colName)
	return &mongoDoc, revision, nil
}

func encodeForJSON(str string) (string, error) {
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(str); err != nil {
		return "", errors.Wrap(err, "error encoding json data")
	}
	// Encode adds double quotes to string and terminates with \n - stripping them as bytes as they are all ascii(0-127)
	buffer := buf.Bytes()
	return string(buffer[1 : len(buffer)-2]), nil
}

func encodePathElement(str string) string {

	u := &url.URL{}
	u.Path = str
	encodedStr := u.EscapedPath() // url encode using golang url path encoding rules
	encodedStr = strings.Replace(encodedStr, "/", "%2F", -1)
	encodedStr = strings.Replace(encodedStr, "+", "%2B", -1)

	return encodedStr
}

//CreateConnectionClient for a new client connection
func (mongoConnectionDef *MongoConnectionDef) CreateConnectionClient() (*mongo.Client, error) {

	logger.Debugf("Entering CreateConnectionClient()")

	var client *mongo.Client
	var err error

	if mongoConnectionDef.Username == "" || mongoConnectionDef.Password == "" {
		client, err = mongo.NewClient(options.Client().ApplyURI(mongoConnectionDef.URL))
	} else {
		client, err = mongo.NewClient(options.Client().SetAuth(
			options.Credential{
				Username:   mongoConnectionDef.Username,
				Password:   mongoConnectionDef.Password,
				AuthSource: mongoConnectionDef.AuthSource}).
			ApplyURI(mongoConnectionDef.URL))
	}

	if err != nil {
		logger.Errorf("CreateConnectionClient error: %s", err)
		return nil, errors.Wrap(err, "CreateConnectionClient error")
	}

	logger.Debugf("Exiting CreateConnectionClient()")

	//return an object containing the connection information
	return client, nil
}

//CreateConnectionDefinition for a new client connection Definition
func CreateConnectionDefinition(mongoDBAddress, username, password string, authSource string, databaseName string, maxRetries,
	maxRetriesOnStartup int, requestTimeout time.Duration) (*MongoConnectionDef, error) {

	logger.Debugf("Entering CreateConnectionDefinition()")

	connectURL := &url.URL{
		Host:   mongoDBAddress,
		Scheme: "mongodb",
	}

	//parse the constructed URL to verify no errors
	finalURL, err := url.Parse(connectURL.String())
	if err != nil {
		logger.Errorf("URL parse error: %s", err)
		return nil, errors.Wrapf(err, "error parsing connect URL: %s", connectURL)
	}

	logger.Debugf("Created database configuration  URL=[%s]", finalURL.String())
	logger.Debugf("Exiting CreateConnectionDefinition()")

	//return an object containing the connection information
	return &MongoConnectionDef{finalURL.String(), username, password, authSource, databaseName, maxRetries,
		maxRetriesOnStartup, requestTimeout}, nil
}

func (dbclient *MongoDatabase) SaveDoc(id string, rev string, mongoDoc *MongoDoc) (string, error) {
	var docMetaData *DocMetadata
	var revision string
	//revisionConflictDetected := false
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	defer dbclient.MongoInstance.recordMetric(time.Now(), colName, "SaveDoc")
	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering SaveDoc() id=[%s]", dbName, colName, id)

	if !utf8.ValidString(id) {
		return "", errors.Errorf("doc id [%x] not a valid utf8 string", id)
	}

	binaryDataJSONMap := make(map[string]interface{})

	if mongoDoc.BinaryDatas == nil {
		//Test to see if this is a valid JSON
		if IsJSON(string(mongoDoc.JSONValue)) != true {
			return "", errors.New("JSON format is not valid")
		}
		// if there are no binaryDatas, then use the bytes passed in as the JSON
		genericMap := make(map[string]interface{})
		//unmarshal the data into the generic map
		decoder := json.NewDecoder(bytes.NewBuffer(mongoDoc.JSONValue))
		decoder.UseNumber()
		_ = decoder.Decode(&genericMap)
		//add all key/values to the binaryDataJSONMap
		for jsonKey, jsonValue := range genericMap {
			binaryDataJSONMap[jsonKey] = jsonValue
		}

	} else { // there are BinaryDatas

		binaryDataJSONMap[binaryField] = mongoDoc.BinaryDatas

		if mongoDoc.JSONValue != nil {
			//create a generic map
			genericMap := make(map[string]interface{})
			//unmarshal the data into the generic map
			decoder := json.NewDecoder(bytes.NewBuffer(mongoDoc.JSONValue))
			decoder.UseNumber()
			_ = decoder.Decode(&genericMap)
			//add all key/values to the binaryDataJSONMap
			for jsonKey, jsonValue := range genericMap {
				binaryDataJSONMap[jsonKey] = jsonValue
			}
		}

	}

	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	//if the revision was not passed in, or if a revision conflict is detected on prior attempt,
	//query MongoDB for the document revision
	if rev == "" {
		rev = dbclient.getDocumentRevision(id)
	}
	opt := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var revN int
	if rev == "" {
		revN = 0
	} else {
		revN, _ = strconv.Atoi(rev)
	}
	res := client.Database(dbName).Collection(colName).FindOneAndUpdate(ctx, bson.M{"_id": id, "_rev": revN}, bson.M{"$inc": bson.M{"_rev": 1}, "$set": binaryDataJSONMap}, opt)
	if res.Err() != nil {
		return "", errors.Wrap(res.Err(), "error SaveDoc")
	}

	err := res.Decode(&docMetaData)
	if err != nil {
		return "", errors.Wrap(err, "error Decode")
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting SaveDoc()", dbName, colName)

	return revision, nil
}

//DeleteDoc method provides function to delete a document from the database by id
func (dbclient *MongoDatabase) DeleteDoc(id, rev string) error {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering DeleteDoc() id=[%s]", dbName, colName, id)

	_, err := client.Database(dbName).Collection(colName, nil).DeleteOne(ctx, bson.M{"_id": id, "_rev": rev})

	if err != nil {
		return err
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting DeleteDoc()", dbName, colName, id)

	return nil

}

//BatchRetrieveDocumentMetadata - batch method to retrieve document metadata for  a set of keys,
// including ID, revision number, and ledger version
func (dbclient *MongoDatabase) BatchRetrieveDocumentMetadata(keys []string) ([]*DocMetadata, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering BatchRetrieveDocumentMetadata()  keys=%s", dbclient.DatabaseName, dbclient.CollectionName, keys)

	//keymap := make(map[string]interface{})
	//keymap["keys"] = keys

	cursor, _ := client.Database(dbName).Collection(colName, nil).Find(ctx, bson.D{{idField, bson.D{{"$in", keys}}}})

	docMetadataArray := []*DocMetadata{}

	//for _, row := range jsonResponse.Rows {
	//	docMetadata := &DocMetadata{ID: row.ID, Rev: row.DocMetadata.Rev, Version: row.DocMetadata.Version}
	//	docMetadataArray = append(docMetadataArray, docMetadata)
	//}

	for cursor.Next(context.TODO()) {
		var docMetadata = &DocMetadata{}
		err := cursor.Decode(docMetadata)
		if err != nil {

		}

		docMetadataArray = append(docMetadataArray, docMetadata)
	}

	logger.Debugf("[%s] Exiting BatchRetrieveDocumentMetadata()", dbclient.DatabaseName)

	return docMetadataArray, nil

}

//BatchUpdateDocuments - batch method to batch update documents
func (dbclient *MongoDatabase) BatchUpdateDocuments(documents []*MongoDoc) ([]*BatchUpdateResponse, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	if logger.IsEnabledFor(zapcore.DebugLevel) {
		documentIdsString, err := printDocumentIds(documents)
		if err == nil {
			logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering BatchUpdateDocuments()  document ids=[%s]", dbName, colName, documentIdsString)
		} else {
			logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering BatchUpdateDocuments()  Could not print document ids due to error: %+v", dbName, colName, err)
		}
	}

	var resultMap []interface{}

	opt := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	for _, jsonDocument := range documents {
		//create a document map
		var document = make(map[string]interface{})

		//unmarshal the JSON component of the MongoDoc into the document
		err := bson.UnmarshalExtJSON(jsonDocument.JSONValue, false, &document)
		if err != nil {
			return nil, errors.Wrap(err, "error unmarshalling json data")
		}

		id := document[idField]
		delete(document, revField)
		deleted := document[deletedField]
		document[binaryField] = jsonDocument.BinaryDatas

		if deleted != nil && deleted == true {
			result := client.Database(dbName).Collection(colName, nil).FindOneAndDelete(ctx, bson.M{"_id": id})
			_, err := result.DecodeBytes()
			if result.Err() != nil {
				if strings.Contains(result.Err().Error(), "no documents in result") {
					continue
				}
				return nil, errors.Wrap(err, "error FindOneAndDelete")
			}
			resultMap = append(resultMap, result)
		} else {
			result := client.Database(dbName).Collection(colName, nil).FindOneAndUpdate(ctx, bson.M{"_id": id}, bson.M{"$inc": bson.M{"_rev": 1}, "$set": document}, opt)
			if result.Err() != nil {
				return nil, errors.Wrap(err, "error FindOneAndUpdate")
			}
			resultMap = append(resultMap, result)
		}

	}

	var response []*BatchUpdateResponse
	var raw []string
	for _, sr := range resultMap {
		var value = &BatchUpdateResponse{}
		rep := sr.(*mongo.SingleResult)
		err := rep.Decode(value)
		if err != nil {
			value.Error = "error"
			value.Reason = err.Error()
		}
		response = append(response, value)
		str := fmt.Sprintf("ID : [%s] Rev : [%d] Error : [%s] Reason : [%s]", value.ID, value.Rev, value.Error, value.Reason)
		raw = append(raw, str)
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting BatchUpdateDocuments()  response=[\n %s \n]", dbName, colName, strings.Join(raw, "\n"))

	return response, nil
}

//getDocumentRevision will return the revision if the document exists, otherwise it will return ""
func (dbclient *MongoDatabase) getDocumentRevision(id string) string {

	var rev = ""

	//See if the document already exists, we need the rev for saves and deletes
	_, revdoc, err := dbclient.ReadDoc(id)
	if err == nil {
		//set the revision to the rev returned from the document read
		rev = revdoc
	}
	return rev
}

// printDocumentIds is a convenience method to print readable log entries for arrays of pointers
// to mongo document IDs
func printDocumentIds(documentPointers []*MongoDoc) (string, error) {

	documentIds := []string{}

	for _, documentPointer := range documentPointers {
		docMetadata := &DocMetadata{}
		err := json.Unmarshal(documentPointer.JSONValue, &docMetadata)
		if err != nil {
			return "", errors.Wrap(err, "error unmarshalling json data")
		}
		documentIds = append(documentIds, docMetadata.ID)
	}
	return strings.Join(documentIds, ","), nil
}

//ReadDocRange method provides function to a range of documents based on the start and end keys
//startKey and endKey can also be empty strings.  If startKey and endKey are empty, all documents are returned
//This function provides a limit option to specify the max number of entries and is supplied by config.
//Skip is reserved for possible future future use.
func (dbclient *MongoDatabase) ReadDocRange(startKey, endKey string, limit int32) ([]*QueryResult, string, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering ReadDocRange()  startKey=%s, endKey=%s", dbName, colName, startKey, endKey)

	var results []*QueryResult

	filter := bson.D{}

	//Append the startKey if provided
	if startKey != "" && endKey != "" {
		filter = bson.D{{"_id", bson.D{{"$gte", startKey}, {"$lt", endKey}}}}
	} else {
		if startKey != "" {
			filter = bson.D{{"_id", bson.D{{"$gte", startKey}}}}
		}
		if endKey != "" {
			filter = bson.D{{"_id", bson.D{{"$lt", endKey}}}}
		}
	}

	findOpt := options.Find().SetLimit(int64(limit + 1)).SetSort(bson.M{"_id": 1})
	cntDoc, _ := client.Database(dbName).Collection(colName).CountDocuments(ctx, filter)
	resultCur, _ := client.Database(dbName).Collection(colName).Find(ctx, filter, findOpt)

	var response = &RangeQueryResponse{}
	response.TotalRows = cntDoc

	//if an additional record is found, then reduce the count by 1
	//and populate the nextStartKey
	if response.TotalRows > int64(limit) {
		response.TotalRows = int64(limit)
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Total Rows: %d", dbclient.DatabaseName, dbclient.CollectionName, response.TotalRows)

	//var addDocument *QueryResult
	//Use the next endKey as the starting default for the nextStartKey
	nextStartKey := endKey
	index := int64(0)

	for resultCur.Next(context.TODO()) {
		var docMetaData *DocMetadata
		err := resultCur.Decode(&docMetaData)
		if err != nil {
			return nil, "", errors.Wrap(err, "error Decode")
		}

		//if there is an extra row for the nextStartKey, then do not add the row to the result set
		//and populate the nextStartKey variable
		if index >= response.TotalRows {
			nextStartKey = docMetaData.ID
			continue
		}
		index++

		var jsonValue = make(map[string]interface{})
		jsonRaw := resultCur.Current
		elem, _ := jsonRaw.Elements()
		for _, value := range elem {
			key := value.Key()
			if key == idField || key == revField || key == versionField || key == binaryField {
				continue
			}

			if value.Value().IsNumber() {
				if _, ok := value.Value().Int64OK(); ok {
					jsonValue[key] = value.Value().Int64()
				} else {
					jsonValue[key] = value.Value().Int32()
				}
			} else {
				jsonValue[key] = value.Value().String()
			}
		}

		jsonValue[idField] = docMetaData.ID
		jsonValue[revField] = docMetaData.Rev
		versionRune := []rune(docMetaData.Version)
		jsonValue[versionField] = string(versionRune)

		data, err := json.Marshal(jsonValue)
		if err != nil {
			return nil, "", errors.Wrap(err, "error marshalling json data")
		}
		var binaryDatas []*BinaryDataInfo
		for _, binaryData := range docMetaData.BinaryDatas {
			binaryDatas = append(binaryDatas, binaryData)
		}

		var addDocument = &QueryResult{docMetaData.ID, data, binaryDatas}
		results = append(results, addDocument)

	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting ReadDocRange()", dbName, colName)

	return results, nextStartKey, nil
}

// CreateIndex method provides a function creating an index
func (dbclient *MongoDatabase) CreateIndex(indexdefinition string) (string, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering CreateIndex()  indexdefinition=%s", dbName, colName, indexdefinition)

	//Test to see if this is a valid JSON
	if IsJSON(indexdefinition) != true {
		return "", errors.New("JSON format is not valid")
	}

	var indexJson ChainCodeMongoIndex
	err := json.Unmarshal([]byte(indexdefinition), &indexJson)
	if err != nil {
		return "", errors.New("error unmarshalling json data")
	}

	if indexJson.Index == nil {
		return "", errors.New("Please check the \"index:\" value")
	}

	if _, ok := indexJson.Index["fields"]; !ok {
		return "", errors.New("Please check the \"fields:\" value")
	}

	fields := indexJson.Index["fields"].([]interface{})

	indexKey := make(map[string]interface{})
	for _, v := range fields {
		switch v.(type) {
		case string:
			indexKey[v.(string)] = ""
		case map[string]interface{}:
			indexKeyMap := v.(map[string]interface{})
			for iKey, iValue := range indexKeyMap {
				indexKey[iKey] = int(iValue.(float64))
			}
		}

	}

	listIdx, err := dbclient.ListIndex()
	if err != nil {
		return "", err
	}

	var result string
	var opt *options.IndexOptions

	for _, v := range listIdx {
		// There is no index update API, so if you have the same index, drop and recreate it.
		if v.Name == indexJson.Name {
			err := dbclient.DropIndex(indexJson.Name)
			if err != nil {
				return "", err
			}
			result = "updated"
		}
	}

	if indexJson.Name == "" {
		// If there is no index name, index name is automatically generated in the form of key_value.
		opt = options.Index().SetUnique(indexJson.Unique)
	} else {
		opt = options.Index().SetName(indexJson.Name).SetUnique(indexJson.Unique)
	}

	indexView := client.Database(dbName).Collection(colName, nil).Indexes()
	IndexModel := mongo.IndexModel{Keys: indexKey, Options: opt}
	indexName, err := indexView.CreateOne(ctx, IndexModel)

	if err != nil {
		return "", errors.Wrapf(err, "error create index [%s]", indexName)
	}

	if result != "updated" {
		result = "created"
		logger.Infof("Created MongoDB index [%s] in state Database Name : [%s] Collection Name : [%s] ", indexName, dbName, colName)
	} else {
		logger.Infof("Updated MongoDB index [%s] in state Database Name : [%s] Collection Name : [%s] ", indexName, dbName, colName)
	}

	return result, nil
}

func (dbclient *MongoDatabase) DropIndex(indexName string) error {
	// _id_ is default index.
	if indexName == "_id_" {
		return nil
	}

	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering DropIndex() index name=%s", dbName, colName, indexName)

	indexView := client.Database(dbName).Collection(colName).Indexes()

	_, err := indexView.DropOne(ctx, indexName)
	if err != nil {
		logger.Errorf("Index Drop error: %s", err)
		return errors.Wrapf(err, "error drop Index [%s]", indexName)
	}

	return nil
}

//QueryDocuments method provides function for processing a query
func (dbclient *MongoDatabase) QueryDocuments(query string) ([]*QueryResult, string, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering QueryDocuments() query=%s", dbName, colName, query)

	queryByte := []byte(query)
	if !IsJson(queryByte) {
		return nil, "", fmt.Errorf("the query is not a json : %s", query)
	}

	mongoQuery := &MongoQuery{}
	err := json.Unmarshal(queryByte, mongoQuery)
	if err != nil {
		return nil, "", fmt.Errorf("the query string is not a query json string:" + err.Error())
	}

	var results []*QueryResult
	returnSkip := mongoQuery.Limit
	cntDoc, _ := client.Database(dbName).Collection(colName).CountDocuments(ctx, mongoQuery.Query)
	findOpt := options.Find()

	if mongoQuery.Limit != 0 {
		findOpt = findOpt.SetLimit(int64(mongoQuery.Limit))
	}
	if mongoQuery.Skip != "" {
		skip, _ := strconv.Atoi(mongoQuery.Skip)
		skipInt64 := int64(skip)
		if skipInt64 > cntDoc {
			returnSkip = skip
		} else {
			returnSkip = skip + mongoQuery.Limit
		}
		findOpt = findOpt.SetSkip(int64(skip))
	}
	if mongoQuery.Sort != nil {
		findOpt = findOpt.SetSort(mongoQuery.Sort)
	}
	if mongoQuery.Projection != nil {
		findOpt = findOpt.SetProjection(mongoQuery.Projection)
	}
	if mongoQuery.Hint != nil {
		findOpt = findOpt.SetHint(mongoQuery.Hint)
	}

	resultCur, err := client.Database(dbName).Collection(colName).Find(ctx, mongoQuery.Query, findOpt)
	if err != nil {
		return nil, "", err
	}
	if resultCur.Err() != nil {
		return nil, "", resultCur.Err()
	}

	for resultCur.Next(context.TODO()) {
		var docMetadata = &DocMetadata{}

		err := resultCur.Decode(&docMetadata)
		if err != nil {
			if err != nil {
				if strings.Contains(err.Error(), "no documents in result") {
					logger.Debugf("Database Name : [%s] Collection Name : [%s] Document not found", dbclient.DatabaseName, dbclient.CollectionName)
					return nil, "", nil
				}
				return nil, "", err
			}
		}

		var docMap map[string]interface{}
		err = resultCur.Decode(&docMap)
		if err != nil {
			return nil, "", errors.Wrap(err, "error Decode")
		}

		resultValue, _ := json.Marshal(docMap)
		var binaryDatas []*BinaryDataInfo
		for _, binaryData := range docMetadata.BinaryDatas {
			binaryDatas = append(binaryDatas, binaryData)
		}

		var addDocument = &QueryResult{docMetadata.ID, resultValue, binaryDatas}
		results = append(results, addDocument)
	}

	var response = &RangeQueryResponse{}
	response.TotalRows = cntDoc

	//if an additional record is found, then reduce the count by 1
	//and populate the nextStartKey
	if response.TotalRows > int64(mongoQuery.Limit) {
		response.TotalRows = int64(mongoQuery.Limit)
	}

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Exiting QueryDocuments()", dbName, colName)
	return results, strconv.Itoa(returnSkip), nil
}

// ListIndex method lists the defined indexes for a database
func (dbclient *MongoDatabase) ListIndex() ([]*MongoIndex, error) {
	dbName := dbclient.DatabaseName
	colName := dbclient.CollectionName
	client := dbclient.MongoInstance.client
	ctx, _ := context.WithTimeout(context.Background(), dbclient.MongoInstance.conf.RequestTimeout)
	client.Connect(ctx)

	logger.Debugf("Database Name : [%s] Collection Name : [%s] Entering ListIndex()", dbName, colName)

	indexView := client.Database(dbName).Collection(colName).Indexes()

	var results []*MongoIndex
	cursor, _ := indexView.List(context.Background())
	for cursor.Next(context.Background()) {
		var idx MongoIndex
		err := cursor.Decode(&idx)
		if err != nil {
			return nil, errors.Wrap(err, "error decode Index")
		}

		// Excludes default indexes.
		if idx.Name == "_id_" {
			continue
		}

		results = append(results, &idx)
	}

	logger.Debugf("Database Name : [%s] Collection Name :  [%s] Exiting ListIndex()", dbName, colName)

	return results, nil

}

//VerifyMongoConfig method provides function to verify the connection information
func (mongoInstance *MongoInstance) VerifyMongoConfig() error {

	logger.Debugf("Entering VerifyMongoConfig()")
	defer logger.Debugf("Exiting VerifyMongoConfig()")

	//set initial wait duration for retries
	waitDuration := retryWaitTime * time.Millisecond

	//get the number of retries for startup
	maxRetriesOnStartup := mongoInstance.conf.MaxRetriesOnStartup
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	for attempts := 0; attempts <= maxRetriesOnStartup; attempts++ {
		_ = mongoInstance.client.Connect(ctx)
		err := mongoInstance.client.Ping(ctx, nil)
		if err != nil {
			//Log the error with the retry count and continue
			logger.Warningf("Retrying mongodb connect in %s. Attempt:%v  Error:%v", waitDuration.String(), attempts+1, err.Error())
			//sleep for specified sleep time, then retry
			time.Sleep(waitDuration)

			//backoff, doubling the retry time for next attempt
			waitDuration *= 2
		}

	}
	return nil
}

func (ci *MongoInstance) recordMetric(startTime time.Time, dbName, api string) {
	ci.stats.observeProcessingTime(startTime, dbName, api, "0")
}

//IsJSON tests a string to determine if a valid JSON
func IsJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}
