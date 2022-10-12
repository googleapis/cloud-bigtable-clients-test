// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"testing"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// TestSampleRowKeys_NoRetry_NoEmptyKey tests that client should accept a list with no empty key.
func TestSampleRowKeys_NoRetry_NoEmptyKey(t *testing.T) {
	// 1. Instantiate the mock server
	sequence := []sampleRowKeysAction{
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30},
		sampleRowKeysAction{rowKey: []byte("row-98"), offsetBytes: 65},
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFn(nil, sequence)

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
	}

	// 3. Perform the operation via test proxy
	res := doSampleRowKeysOp(t, server, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(res.GetSample()))
	assert.Equal(t, "row-31", string(res.GetSample()[0].RowKey))
	assert.Equal(t, "row-98", string(res.GetSample()[1].RowKey))
}

// TestSampleRowKeys_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestSampleRowKeys_Generic_MultiStreams(t *testing.T) {
	// 0. Common variable
	const concurrency = 5
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, requestRecorderCapacity)
	actions := make([]sampleRowKeysAction, concurrency)
	for i := 0; i < concurrency; i++ {
		// Each request will get the same response.
		actions[i] = sampleRowKeysAction{
			rowKey: []byte("row-31"), offsetBytes: 30, endOfStream: true, delayStr: "2s"}
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFn(recorder, actions)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.SampleRowKeysRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		// The same client request is used for the concurrent operations.
		reqs[i] = &testproxypb.SampleRowKeysRequest{
			ClientId:  t.Name(),
			Request:  &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
		}
	}

	// 3. Perform the operations via test proxy
	results := doSampleRowKeysOps(t, server, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)
}

