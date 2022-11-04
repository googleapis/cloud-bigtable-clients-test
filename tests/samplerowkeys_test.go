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
	"time"

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

// TestSampleRowKeys_GenericClientGap_CloseClient tests that client doesn't kill inflight requests after
// client closing, but will reject new requests.
func TestSampleRowKeys_GenericClientGap_CloseClient(t *testing.T) {
	// 0. Common variable
	halfBatchSize := 3
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, requestRecorderCapacity)
	actions := make([]sampleRowKeysAction, 2 * halfBatchSize)
	for i := 0; i < 2 * halfBatchSize; i++ {
		// Each request will get the same response.
		actions[i] = sampleRowKeysAction{
			rowKey: []byte("row-31"), offsetBytes: 30, endOfStream: true, delayStr: "2s"}
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFn(recorder, actions)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.SampleRowKeysRequest, halfBatchSize) // Will be finished
	reqsBatchTwo := make([]*testproxypb.SampleRowKeysRequest, halfBatchSize) // Will be rejected by client
	for i := 0; i < halfBatchSize; i++ {
		reqsBatchOne[i] = &testproxypb.SampleRowKeysRequest{
			ClientId: clientID,
			Request: &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
		}
		reqsBatchTwo[i] = &testproxypb.SampleRowKeysRequest{
			ClientId: clientID,
			Request: &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
		}
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doSampleRowKeysOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doSampleRowKeysOpsCore(t, clientID, reqsBatchTwo, nil)

	// 4a. Check that server only receives batch-one requests
	assert.Equal(t, halfBatchSize, len(recorder))

	// 4b. Check that all the batch-one requests succeeded
	checkResultOkStatus(t, resultsBatchOne...)

	// 4c. Check that all the batch-two requests failed at the proxy level:
	// the proxy tries to use close client. Client and server have nothing to blame.
	// We are a little permissive here by just checking if failures occur.
	for i := 0; i < halfBatchSize; i++ {
		if resultsBatchTwo[i] == nil {
			continue
		}
		assert.NotEmpty(t, resultsBatchTwo[i].GetStatus().GetCode())
	}
}

