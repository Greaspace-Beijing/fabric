/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statemongodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util/mongodb"
)

var logger = flogging.MustGetLogger("statemongodb")

// LsccCacheSize denotes the number of entries allowed in the lsccStateCache
const lsccCacheSize = 50

const savepointDocID = "statedb_savepoint"

var savePointNs = "savepoint"

var queryskip = 0

//MongoConnectionDef contains parameters
type MongoConnectionDef struct {
	URL                 string
	Username            string
	Password            string
	AuthSource          string
	MaxRetries          int
	MaxRetriesOnStartup int
	RequestTimeout      time.Duration
}

type VersionedDBProvider struct {
	mongoInstance *mongodb.MongoInstance
	databases     map[string]*VersionedDB
	mux           sync.Mutex
	openCounts    uint64
}

type VersionedDB struct {
	mongoInstance      *mongodb.MongoInstance
	metadataDB         *mongodb.MongoDatabase
	chainName          string
	namespaceDBs       map[string]*mongodb.MongoDatabase
	committedDataCache *versionsCache // Used as a local cache during bulk processing of a block.
	verCacheLock       sync.RWMutex
	mux                sync.RWMutex
	lsccStateCache     *lsccStateCache
}

type lsccStateCache struct {
	cache   map[string]*statedb.VersionedValue
	rwMutex sync.RWMutex
}

type queryScanner struct {
	namespace       string
	db              *mongodb.MongoDatabase
	queryDefinition *queryDefinition
	paginationInfo  *paginationInfo
	resultsInfo     *resultsInfo
}

type queryDefinition struct {
	startKey   string
	endKey     string
	query      string
	queryLimit int32
}

type paginationInfo struct {
	cursor int32
	limit  int32
	skip   string
}

type resultsInfo struct {
	totalRecordsReturned int32
	results              []*mongodb.QueryResult
}

func (l *lsccStateCache) getState(key string) *statedb.VersionedValue {
	l.rwMutex.RLock()
	defer l.rwMutex.RUnlock()

	if versionedValue, ok := l.cache[key]; ok {
		logger.Debugf("key:[%s] found in the lsccStateCache", key)
		return versionedValue
	}
	return nil
}

func (l *lsccStateCache) updateState(key string, value *statedb.VersionedValue) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()

	if _, ok := l.cache[key]; ok {
		logger.Debugf("key:[%s] is updated in lsccStateCache", key)
		l.cache[key] = value
	}
}

func (l *lsccStateCache) setState(key string, value *statedb.VersionedValue) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()

	if l.isCacheFull() {
		l.evictARandomEntry()
	}

	logger.Debugf("key:[%s] is stoed in lsccStateCache", key)
	l.cache[key] = value
}

func (l *lsccStateCache) isCacheFull() bool {
	return len(l.cache) == lsccCacheSize
}

func (l *lsccStateCache) evictARandomEntry() {
	for key := range l.cache {
		delete(l.cache, key)
		return
	}
}

func NewVersionedDBProvider(metricsProvider metrics.Provider) (*VersionedDBProvider, error) {
	logger.Debugf("constructing MongoDB VersionedDBProvider")
	mongodbConf := mongodb.GetMongoDBDefinition()
	mongodbInstance, err := mongodb.CreateMongoInstance(mongodbConf.URL, mongodbConf.Username, mongodbConf.Password, mongodbConf.AuthSource,
		mongodbConf.DatabaseName, mongodbConf.MaxRetries, mongodbConf.MaxRetriesOnStartup, mongodbConf.RequestTimeout, metricsProvider)

	if err != nil {
		return nil, err
	}

	return &VersionedDBProvider{mongodbInstance, make(map[string]*VersionedDB), sync.Mutex{}, 0}, nil
}

func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()

	vdb := provider.databases[dbName]
	if vdb == nil {
		var err error
		vdb, err = newVersionedDB(provider.mongoInstance, dbName)
		if err != nil {
			return nil, err
		}
		provider.databases[dbName] = vdb
	}

	return vdb, nil
}

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (vdb *VersionedDB) ProcessIndexesForChaincodeDeploy(namespace string, fileEntries []*ccprovider.TarFileEntry) error {
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		return err
	}
	for _, fileEntry := range fileEntries {
		indexData := fileEntry.FileContent
		filename := fileEntry.FileHeader.Name
		_, err = db.CreateIndex(string(indexData))
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf(
				"error creating index from file [%s] for channel [%s]", filename, namespace))
		}
	}
	return nil
}

func (provider *VersionedDBProvider) Close() {
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Debugf("statemongodb.go GetState() namespace : %s, key : %s", namespace, key)
	if namespace == "lscc" {
		if value := vdb.lsccStateCache.getState(key); value != nil {
			return value, nil
		}
	}

	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		return nil, err
	}
	mongoDoc, _, err := db.ReadDoc(key)
	if mongoDoc == nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	kv, err := mongoDocToKeyValue(mongoDoc)
	if err != nil {
		return nil, err
	}

	if namespace == "lscc" {
		vdb.lsccStateCache.setState(key, kv.VersionedValue)
	}

	return kv.VersionedValue, nil
}

// GetVersion implements method in VersionedDB interface
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	logger.Debugf("statemongodb.go GetVersion namespace : %s  key : ", namespace, key)
	returnVersion, keyFound := vdb.GetCachedVersion(namespace, key)
	if !keyFound {
		// This if block get executed only during simulation because during commit
		// we always call `LoadCommittedVersions` before calling `GetVersion`
		vv, err := vdb.GetState(namespace, key)
		if err != nil || vv == nil {
			return nil, err
		}
		returnVersion = vv.Version
	}
	return returnVersion, nil
}

// GetCachedVersion returns version from cache. `LoadCommittedVersions` function populates the cache
func (vdb *VersionedDB) GetCachedVersion(namespace string, key string) (*version.Height, bool) {
	logger.Debugf("Retrieving cached version: %s~%s", key, namespace)
	vdb.verCacheLock.RLock()
	defer vdb.verCacheLock.RUnlock()
	return vdb.committedDataCache.getVersion(namespace, key)
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return vdb.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)

}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	queryResult, err := vdb.ExecuteQueryWithMetadata(namespace, query, nil)
	if err != nil {
		return nil, err
	}
	return queryResult, nil
}

func (vdb *VersionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	logger.Debugf("Entering ExecuteQueryWithMetadata  namespace: %s,  query: %s,  metadata: %v", namespace, query, metadata)
	// Get the querylimit from core.yaml
	queryLimit := int32(ledgerconfig.GetMongodbQueryLimit())
	limit := int32(0)
	skip := ""
	// if metadata is provided, then validate and set provided options
	if metadata != nil {
		err := validateQueryMetadata(metadata)
		if err != nil {
			return nil, err
		}
		if limitOption, ok := metadata[optionLimit]; ok {
			limit = limitOption.(int32)
		}
		if skipOption, ok := metadata[optionSkip]; ok {
			skip = skipOption.(string)
		}
	}
	queryString, err := applyAdditionalQueryOptions(query, queryLimit, skip)
	if err != nil {
		logger.Errorf("Error calling applyAdditionalQueryOptions(): %s", err.Error())
		return nil, err
	}
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		return nil, err
	}
	return newQueryScanner(namespace, db, queryString, queryLimit, limit, skip, "", "")
}

func validateQueryMetadata(metadata map[string]interface{}) error {
	for key, keyVal := range metadata {
		switch key {
		case optionSkip:
			if _, ok := keyVal.(string); ok {
				continue
			}
			return fmt.Errorf("Invalid entry, \"skip\" must be an string")

		case optionLimit:
			//Verify the limit is an integer
			if _, ok := keyVal.(int32); ok {
				continue
			}
			return fmt.Errorf("Invalid entry, \"limit\" must be an int32")

		default:
			return fmt.Errorf("Invalid entry, option %s not recognized", key)
		}
	}
	return nil
}

// applyAdditionalQueryOptions will add additional fields to the query required for query processing
func applyAdditionalQueryOptions(queryString string, queryLimit int32, querySkip string) (string, error) {
	const jsonQueryFields = "fields"
	const jsonQueryLimit = "limit"
	const jsonQuerySkip = "skip"

	//create a generic map for the query json
	jsonQueryMap := make(map[string]interface{})
	//unmarshal the selector json into the generic map
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(queryString)))
	decoder.UseNumber()
	err := decoder.Decode(&jsonQueryMap)
	if err != nil {
		return "", err
	}
	if fieldsJSONArray, ok := jsonQueryMap[jsonQueryFields]; ok {
		switch fieldsJSONArray.(type) {
		case []interface{}:
			//Add the "_id", and "version" fields,  these are needed by default
			jsonQueryMap[jsonQueryFields] = append(fieldsJSONArray.([]interface{}),
				idField, versionField)
		default:
			return "", errors.New("fields definition must be an array")
		}
	}
	// Add limit
	// This will override any limit passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQueryLimit] = queryLimit
	// Add the skip if provided
	if querySkip != "" {
		jsonQueryMap[jsonQuerySkip] = querySkip
	}
	//Marshal the updated json query
	editedQuery, err := json.Marshal(jsonQueryMap)
	if err != nil {
		return "", err
	}
	logger.Debugf("Rewritten query: %s", editedQuery)
	return string(editedQuery), nil
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(updates *statedb.UpdateBatch, height *version.Height) error {
	// TODO a note about https://jira.hyperledger.org/browse/FAB-8622
	// the function `Apply update can be split into three functions. Each carrying out one of the following three stages`.
	// The write lock is needed only for the stage 2.

	// stage 1 - PrepareForUpdates - db transforms the given batch in the form of underlying db
	// and keep it in memory
	var updateBatches []batch
	var err error
	if updateBatches, err = vdb.buildCommitters(updates); err != nil {
		return err
	}
	// stage 2 - ApplyUpdates push the changes to the DB
	if err = executeBatches(updateBatches); err != nil {
		return err
	}

	// Stgae 3 - PostUpdateProcessing - flush and record savepoint.
	namespaces := updates.GetUpdatedNamespaces()
	// Record a savepoint at a given height
	if err = vdb.ensureFullCommitAndRecordSavepoint(height, namespaces); err != nil {
		logger.Errorf("Error during recordSavepoint: %s", err.Error())
		return err
	}

	lsccUpdates := updates.GetUpdates("lscc")
	for key, value := range lsccUpdates {
		vdb.lsccStateCache.updateState(key, value)
	}

	return nil
}

func (vdb *VersionedDB) ensureFullCommitAndRecordSavepoint(height *version.Height, namespaces []string) error {
	// ensure full commit to flush all changes on updated namespaces until now to disk
	// namespace also includes empty namespace which is nothing but metadataDB
	var dbs []*mongodb.MongoDatabase
	for _, ns := range namespaces {
		db, err := vdb.getNamespaceDBHandle(ns)
		if err != nil {
			return err
		}
		dbs = append(dbs, db)
	}
	if err := vdb.ensureFullCommit(dbs); err != nil {
		return err
	}

	// If a given height is nil, it denotes that we are committing pvt data of old blocks.
	// In this case, we should not store a savepoint for recovery. The lastUpdatedOldBlockList
	// in the pvtstore acts as a savepoint for pvt data.
	if height == nil {
		return nil
	}

	// construct savepoint document and save
	savepointMongoDoc, err := encodeSavepoint(height)
	if err != nil {
		return err
	}
	_, err = vdb.metadataDB.SaveDoc(savepointDocID, "", savepointMongoDoc)
	if err != nil {
		logger.Errorf("Failed to save the savepoint to DB %s", err.Error())
		return err
	}

	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	var err error
	mongoDoc, _, err := vdb.metadataDB.ReadDoc(savepointDocID)
	if err != nil {
		logger.Errorf("Failed to read savepoint data %s", err.Error())
		return nil, err
	}
	if mongoDoc == nil || mongoDoc.JSONValue == nil {
		return nil, nil
	}

	return decodeSavepoint(mongoDoc)
}

// ValidateKey implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKey(key string) error {
	if !utf8.ValidString(key) {
		return fmt.Errorf("Key should be a valid utf8 string: [%x]", key)
	}
	return nil
}

// BytesKeySuppoted implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySupported() bool {
	return false
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {

}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	err := validateKey(key)
	if err != nil {
		return err
	}
	return validateValue(value)
}

//get or create newVersionDB
func newVersionedDB(mongoInstance *mongodb.MongoInstance, colName string) (*VersionedDB, error) {
	chainName := colName
	colName = mongodb.ConstructMetadataCOLName(colName)

	metadataDB, err := mongodb.CreateMongoCollection(mongoInstance, colName)
	if err != nil {
		return nil, err
	}

	namespaceDBMap := make(map[string]*mongodb.MongoDatabase)
	return &VersionedDB{
		mongoInstance:      mongoInstance,
		metadataDB:         metadataDB,
		chainName:          chainName,
		namespaceDBs:       namespaceDBMap,
		committedDataCache: newVersionCache(),
		lsccStateCache: &lsccStateCache{
			cache: make(map[string]*statedb.VersionedValue),
		},
	}, nil
}

// getNamespaceDBHandle gets the handle to a named chaincode database
func (vdb *VersionedDB) getNamespaceDBHandle(namespace string) (*mongodb.MongoDatabase, error) {
	vdb.mux.RLock()
	db := vdb.namespaceDBs[namespace]
	vdb.mux.RUnlock()
	if db != nil {
		return db, nil
	}
	namespaceDBName := mongodb.ConstructNamespaceDBName(vdb.chainName, namespace, vdb.metadataDB.DatabaseName)
	vdb.mux.Lock()
	defer vdb.mux.Unlock()
	db = vdb.namespaceDBs[namespace]
	if db == nil {
		var err error
		db, err = mongodb.CreateMongoCollection(vdb.mongoInstance, namespaceDBName)
		if err != nil {
			return nil, err
		}
		vdb.namespaceDBs[namespace] = db
	}
	return db, nil
}

const optionLimit = "limit"

// should use the skip of mongodb, but use it as a bookmark for compatibility.
const optionSkip = "bookmark"

func (vdb *VersionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	logger.Debugf("Entering GetStateRangeScanIteratorWithMetadata  namespace: %s  startKey: %s  endKey: %s  metadata: %v", namespace, startKey, endKey, metadata)
	// Get the internalQueryLimit from core.yaml
	queryLimit := int32(ledgerconfig.GetMongodbQueryLimit())
	limit := int32(0)
	skip := ""
	// if metadata is provided, validate and apply options
	if metadata != nil {
		//validate the metadata
		err := statedb.ValidateRangeMetadata(metadata)
		if err != nil {
			return nil, err
		}
		if limitOption, ok := metadata[optionLimit]; ok {
			limit = limitOption.(int32)
		}
		if skipOption, ok := metadata[optionSkip]; ok {
			skip = skipOption.(string)
		}
	}
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		return nil, err
	}
	return newQueryScanner(namespace, db, "", queryLimit, limit, skip, startKey, endKey)
}

// executeQueryWithBookmark executes a "paging" query with a skip, this method allows a
// paged query without returning a new query iterator
func (scanner *queryScanner) executeQuery() error {
	queryLimit := scanner.queryDefinition.queryLimit
	if scanner.paginationInfo.limit > 0 {
		if scanner.paginationInfo.limit-scanner.resultsInfo.totalRecordsReturned < scanner.queryDefinition.queryLimit {
			queryLimit = scanner.paginationInfo.limit - scanner.resultsInfo.totalRecordsReturned
		}
	}
	if queryLimit == 0 {
		scanner.resultsInfo.results = nil
		return nil
	}
	queryString, err := applyAdditionalQueryOptions(scanner.queryDefinition.query,
		queryLimit, scanner.paginationInfo.skip)
	if err != nil {
		logger.Debugf("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		return err
	}
	queryResult, skip, err := scanner.db.QueryDocuments(queryString)
	if err != nil {
		logger.Debugf("Error calling QueryDocuments(): %s\n", err.Error())
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.paginationInfo.skip = skip
	scanner.paginationInfo.cursor = 0
	return nil
}

func newQueryScanner(namespace string, db *mongodb.MongoDatabase, query string, queryLimit, limit int32, skip, startKey, endKey string) (*queryScanner, error) {
	scanner := &queryScanner{namespace, db, &queryDefinition{startKey, endKey, query, queryLimit}, &paginationInfo{-1, limit, skip}, &resultsInfo{0, nil}}
	var err error
	// query is defined, then execute the query and return the records and skip
	if scanner.queryDefinition.query != "" {
		err = scanner.executeQuery()
	} else {
		err = scanner.getNextStateRangeScanResults()
	}
	if err != nil {
		return nil, err
	}
	scanner.paginationInfo.cursor = -1
	return scanner, nil
}

func (scanner *queryScanner) getNextStateRangeScanResults() error {
	queryLimit := scanner.queryDefinition.queryLimit

	if scanner.paginationInfo.limit > 0 {
		moreResultsNeeded := scanner.paginationInfo.limit - scanner.resultsInfo.totalRecordsReturned
		if moreResultsNeeded < scanner.queryDefinition.queryLimit {
			queryLimit = moreResultsNeeded
		}
	}
	queryResult, nextStartKey, err := rangeScanFilterMongoInternalDocs(scanner.db, scanner.queryDefinition.startKey, scanner.queryDefinition.endKey, queryLimit)
	if err != nil {
		return err
	}
	scanner.resultsInfo.results = queryResult
	scanner.queryDefinition.startKey = nextStartKey
	scanner.paginationInfo.cursor = 0
	return nil
}

func rangeScanFilterMongoInternalDocs(db *mongodb.MongoDatabase, startKey, endKey string, queryLimit int32) ([]*mongodb.QueryResult, string, error) {
	var finalResults []*mongodb.QueryResult
	var finalNextStartKey string
	for {
		results, nextStartKey, err := db.ReadDocRange(startKey, endKey, queryLimit)
		if err != nil {
			logger.Debugf("Error calling ReadDocRange(): %s\n", err.Error())
			return nil, "", err
		}
		var filteredResults []*mongodb.QueryResult
		for _, doc := range results {
			filteredResults = append(filteredResults, doc)
		}

		finalResults = append(finalResults, filteredResults...)
		finalNextStartKey = nextStartKey
		queryLimit = int32(len(results) - len(filteredResults))
		if queryLimit == 0 || finalNextStartKey == "" {
			break
		}
		startKey = finalNextStartKey
	}
	return finalResults, finalNextStartKey, nil
}

// GetDBType returns the hosted stateDB
func (vdb *VersionedDB) GetDBType() string {
	return "mongodb"
}

func (scanner *queryScanner) Next() (statedb.QueryResult, error) {

	//test for no results case
	if len(scanner.resultsInfo.results) == 0 {
		return nil, nil
	}
	// increment the cursor
	scanner.paginationInfo.cursor++
	// check to see if additional records are needed
	// requery if the cursor exceeds the internalQueryLimit
	if scanner.paginationInfo.cursor >= scanner.queryDefinition.queryLimit {
		var err error
		// query is defined, then execute the query and return the records and skip
		if scanner.queryDefinition.query != "" {
			err = scanner.executeQuery()
		} else {
			err = scanner.getNextStateRangeScanResults()
		}
		if err != nil {
			return nil, err
		}
		//if no more results, then return
		if len(scanner.resultsInfo.results) == 0 {
			return nil, nil
		}
	}
	//If the cursor is greater than or equal to the number of result records, return
	if scanner.paginationInfo.cursor >= int32(len(scanner.resultsInfo.results)) {
		return nil, nil
	}
	selectedResultRecord := scanner.resultsInfo.results[scanner.paginationInfo.cursor]
	key := selectedResultRecord.ID
	// remove the reserved fields from MongoDB JSON and return the value and version
	kv, err := mongoDocToKeyValue(&mongodb.MongoDoc{JSONValue: selectedResultRecord.Value, BinaryDatas: selectedResultRecord.BinaryDatas})
	if err != nil {
		return nil, err
	}
	scanner.resultsInfo.totalRecordsReturned++
	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: *kv.VersionedValue}, nil
}

func (scanner *queryScanner) Close() {
	scanner = nil
}

func (scanner *queryScanner) GetBookmarkAndClose() string {
	retval := ""
	if scanner.queryDefinition.query != "" {
		retval = scanner.paginationInfo.skip
	} else {
		retval = scanner.queryDefinition.startKey
	}
	scanner.Close()
	return retval
}
