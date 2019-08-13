/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package mongodb

import (
	"time"

	"github.com/hyperledger/fabric/common/metrics"
)

var (
	apiProcessingTimeOpts = metrics.HistogramOpts{
		Namespace:    "mongodb",
		Subsystem:    "",
		Name:         "processing_time",
		Help:         "Time taken in seconds for the function to complete request to MongoDB",
		LabelNames:   []string{"database", "function_name", "result"},
		StatsdFormat: "%{#fqname}.%{collection}.%{function_name}",
	}
)

type stats struct {
	apiProcessingTime metrics.Histogram
}

func newStats(metricsProvider metrics.Provider) *stats {
	return &stats{
		apiProcessingTime: metricsProvider.NewHistogram(apiProcessingTimeOpts),
	}
}

func (s *stats) observeProcessingTime(startTime time.Time, colName, functionName, result string) {
	s.apiProcessingTime.With(
		"collection", colName,
		"function_name", functionName,
	).Observe(time.Since(startTime).Seconds())
}
