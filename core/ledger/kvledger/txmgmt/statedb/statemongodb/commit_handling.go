/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statemongodb

import (
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/mongodb"
	"github.com/pkg/errors"
)

// nsCommittersBuilder implements `batch` interface. Each batch operates on a specific namespace in the updates and
// builds one or more batches of type subNsCommitter.
type nsCommittersBuilder struct {
	updates         map[string]*statedb.VersionedValue
	db              *mongodb.MongoDatabase
	revisions       map[string]string
	subNsCommitters []batch
}

// subNsCommitter implements `batch` interface. Each batch commits the portion of updates within a namespace assigned to it
type subNsCommitter struct {
	db             *mongodb.MongoDatabase
	batchUpdateMap map[string]*batchableDocument
}

// buildCommitters build the batches of type subNsCommitter. This functions processes different namespaces in parallel
func (vdb *VersionedDB) buildCommitters(updates *statedb.UpdateBatch) ([]batch, error) {
	namespaces := updates.GetUpdatedNamespaces()

	var nsCommitterBuilder []batch
	for _, ns := range namespaces {
		nsUpdates := updates.GetUpdates(ns)
		db, err := vdb.getNamespaceDBHandle(ns)
		if err != nil {
			return nil, err
		}
		nsRevs := vdb.committedDataCache.revs[ns]
		if nsRevs == nil {
			nsRevs = make(nsRevisions)
		}
		// for each namespace, construct one builder with the corresponding mongodb handle and mongo revisions
		// that are already loaded into cache (during validation phase)
		nsCommitterBuilder = append(nsCommitterBuilder, &nsCommittersBuilder{updates: nsUpdates, db: db, revisions: nsRevs})
	}
	if err := executeBatches(nsCommitterBuilder); err != nil {
		return nil, err
	}
	// accumulate results across namespaces (one or more batches of `subNsCommitter` for a namespace from each builder)
	var combinedSubNsCommitters []batch
	for _, b := range nsCommitterBuilder {
		combinedSubNsCommitters = append(combinedSubNsCommitters, b.(*nsCommittersBuilder).subNsCommitters...)
	}
	return combinedSubNsCommitters, nil
}

// execute implements the function in `batch` interface. This function builds one or more `subNsCommitter`s that
// cover the updates for a namespace
func (builder *nsCommittersBuilder) execute() error {
	if err := addRevisionsForMissingKeys(builder.revisions, builder.db, builder.updates); err != nil {
		return err
	}
	maxBacthSize := ledgerconfig.GetMaxBatchUpdateSize()
	batchUpdateMap := make(map[string]*batchableDocument)
	for key, vv := range builder.updates {
		mongoDoc, err := keyValToMongoDoc(&keyValue{key: key, VersionedValue: vv}, builder.revisions[key])
		if err != nil {
			return err
		}
		batchUpdateMap[key] = &batchableDocument{MongoDoc: *mongoDoc, Deleted: vv.Value == nil}
		if len(batchUpdateMap) == maxBacthSize {
			builder.subNsCommitters = append(builder.subNsCommitters, &subNsCommitter{builder.db, batchUpdateMap})
			batchUpdateMap = make(map[string]*batchableDocument)
		}
	}
	if len(batchUpdateMap) > 0 {
		builder.subNsCommitters = append(builder.subNsCommitters, &subNsCommitter{builder.db, batchUpdateMap})
	}
	return nil
}

// execute implements the function in `batch` interface. This function commits the updates managed by a `subNsCommitter`
func (committer *subNsCommitter) execute() error {
	return commitUpdates(committer.db, committer.batchUpdateMap)
}

// commitUpdates commits the given updates to mongodb
func commitUpdates(db *mongodb.MongoDatabase, batchUpdateMap map[string]*batchableDocument) error {
	//Add the documents to the batch update array
	batchUpdateDocs := []*mongodb.MongoDoc{}
	for _, updateDocument := range batchUpdateMap {
		batchUpdateDocument := updateDocument
		batchUpdateDocs = append(batchUpdateDocs, &batchUpdateDocument.MongoDoc)
	}

	// Do the bulk update into mongodb. Note that this will do retries if the entire bulk update fails or times out
	batchUpdateResp, err := db.BatchUpdateDocuments(batchUpdateDocs)
	if err != nil {
		return err
	}

	// IF INDIVIDUAL DOCUMENTS IN THE BULK UPDATE DID NOT SUCCEED, TRY THEM INDIVIDUALLY
	// iterate through the response from MongoDB by document
	for _, respDoc := range batchUpdateResp {
		// If the document returned an error, retry the individual document
		if respDoc.Error != "" {
			batchUpdateDocument := batchUpdateMap[respDoc.ID]
			var err error
			if batchUpdateDocument.MongoDoc.JSONValue != nil {
				err = removeJSONRevision(&batchUpdateDocument.MongoDoc.JSONValue)
				if err != nil {
					return err
				}
			}
			// Check to see if the document was added to the batch as a delete type document
			if batchUpdateDocument.Deleted {
				logger.Warningf("MongoDB batch document delete encountered an problem. Retrying delete for document ID:%s", respDoc.ID)
				err = db.DeleteDoc(respDoc.ID, "")
			} else {
				logger.Warningf("MongoDB batch document update encountered an problem. Retrying update for document ID:%s", respDoc.ID)
				// Note that this will do retries as needed
				_, err = db.SaveDoc(respDoc.ID, "", &batchUpdateDocument.MongoDoc)
			}

			// If the single document update or delete returns an error, then throw the error
			if err != nil {
				errorString := fmt.Sprintf("error saving document ID: %v. Error: %s,  Reason: %s",
					respDoc.ID, respDoc.Error, respDoc.Reason)

				logger.Errorf(errorString)
				return errors.WithMessage(err, errorString)
			}
		}
	}
	return nil
}

// nsFlusher implements `batch` interface and a batch executes the function `mongodb.EnsureFullCommit()` for the given namespace
type nsFlusher struct {
	db *mongodb.MongoDatabase
}

func (vdb *VersionedDB) ensureFullCommit(dbs []*mongodb.MongoDatabase) error {
	var flushers []batch
	for _, db := range dbs {
		flushers = append(flushers, &nsFlusher{db})
	}
	return executeBatches(flushers)
}

func (f *nsFlusher) execute() error {
	return nil
}

func addRevisionsForMissingKeys(revisions map[string]string, db *mongodb.MongoDatabase, nsUpdates map[string]*statedb.VersionedValue) error {
	var missingKeys []string
	for key := range nsUpdates {
		_, ok := revisions[key]
		if !ok {
			missingKeys = append(missingKeys, key)
		}
	}
	logger.Debugf("Pulling revisions for the [%d] keys for namsespace [%s] that were not part of the readset", len(missingKeys), db.DatabaseName)
	retrievedMetadata, err := retrieveNsMetadata(db, missingKeys)
	if err != nil {
		return err
	}
	for _, metadata := range retrievedMetadata {
		revisions[metadata.ID] = strconv.Itoa(metadata.Rev)
	}
	return nil
}

//batchableDocument defines a document for a batch
type batchableDocument struct {
	MongoDoc mongodb.MongoDoc
	Deleted  bool
}
