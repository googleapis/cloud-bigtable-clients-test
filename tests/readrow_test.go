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

// The test cases in this file will use dummyChunkData() from readrows_test.go.
package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestReadRow_NoRetry_PointReadDeadline tests that client will set deadline for point read.
func TestReadRow_NoRetry_PointReadDeadline(t *testing.T) {
	// 1. Instantiate the mock server
	recorder := make(chan *readRowsReqRecord, 1)
	action := &readRowsAction{
		chunks:   []chunkData{dummyChunkData("row-01", "v1", Commit)},
		delayStr: "5s",
	}
	server := initMockServer(t)
	server.ReadRowsFn = mockReadRowsFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowRequest{
		ClientId:  t.Name(),
		TableName: buildTableName("table"),
		RowKey:    "row-01",
	}

	// 3. Perform the operation via test proxy
	timeout := durationpb.Duration{
		Seconds: 2,
	}
	res := doReadRowOp(t, server, &req, &timeout)

	// 4a. Check the runtime
	curTs := time.Now()
	origReq := <-recorder
	runTimeSecs := int(curTs.Unix() - origReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 5)

	// 4b. Check the DeadlineExceeded error
	assert.Equal(t, int32(codes.DeadlineExceeded), res.GetStatus().GetCode())
}

// TestReadRow_NoRetry_CommitInSeparateChunk tests that client can have one chunk
// with no status and subsequent chunk with a commit status.
func TestReadRow_NoRetry_CommitInSeparateChunk(t *testing.T) {
	// 1. Instantiate the mock server
	recorder := make(chan *readRowsReqRecord, 1)
	action := &readRowsAction{
		chunks: []chunkData{
			chunkData{rowKey: []byte("row-01"), familyName: "A", qualifier: "Qw1", timestampMicros: 99, value: "dmFsdWUtVkFM", status: None},
			chunkData{familyName: "B", qualifier: "Qw2", timestampMicros: 102, value: "dmFsdWUtVkFJ", status: Commit},
		},
	}
	server := initMockServer(t)
	server.ReadRowsFn = mockReadRowsFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowRequest{
		ClientId:  t.Name(),
		TableName: buildTableName("table"),
		RowKey:    "row-01",
	}

	// 3. Perform the operation via test proxy
	res := doReadRowOp(t, server, &req, nil)

	// 4. Verify that the read succeeds
	expectedRow := btpb.Row{
		Key: []byte("row-01"),
		Families: []*btpb.Family{
			&btpb.Family{
				Name: "A",
				Columns: []*btpb.Column{
					&btpb.Column{
						Qualifier: []byte("Qw1"),
						Cells: []*btpb.Cell{
							&btpb.Cell{
								TimestampMicros: 99,
								Value:           []byte("dmFsdWUtVkFM"),
							},
						},
					},
				},
			},
			&btpb.Family{
				Name: "B",
				Columns: []*btpb.Column{
					&btpb.Column{
						Qualifier: []byte("Qw2"),
						Cells: []*btpb.Cell{
							&btpb.Cell{
								TimestampMicros: 102,
								Value:           []byte("dmFsdWUtVkFJ"),
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, "", cmp.Diff(expectedRow, res.Row, protocmp.Transform()))
}

// TestReadRow_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestReadRow_Generic_MultiStreams(t *testing.T) {
	// 0. Common variable
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row"}
	concurrency := len(rowKeys)
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *readRowsReqRecord, requestRecorderCapacity)
	actions := make([]*readRowsAction, concurrency)
	for i := 0; i < concurrency; i++ {
		// Each request will get a different response.
		actions[i] = &readRowsAction{
			chunks:      []chunkData{dummyChunkData(rowKeys[i], fmt.Sprintf("value%d", i), Commit)},
			delayStr:    "2s",
		}
	}
	server := initMockServer(t)
	server.ReadRowsFn = mockReadRowsFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.ReadRowRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		reqs[i] = &testproxypb.ReadRowRequest{
			ClientId:  t.Name(),
			TableName: buildTableName("table"),
			RowKey:    rowKeys[i],
		}
	}

	// 3. Perform the operations via test proxy
	results := doReadRowOps(t, server, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)

	// 4c. Check the row keys in the results.
	for i := 0; i < concurrency; i++ {
		assert.Equal(t, rowKeys[i], string(results[i].Row.Key))
	}
}

// TestReadRow_Generic_CloseClient tests that client doesn't kill inflight requests after client
// closing, but will reject new requests.
func TestReadRow_Generic_CloseClient(t *testing.T) {
	// 0. Common variable
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row", "op5-row"}
	concurrency := len(rowKeys) / 2
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *readRowsReqRecord, requestRecorderCapacity)
	actions := make([]*readRowsAction, 2 * concurrency)
	for i := 0; i < 2 * concurrency; i++ {
		// Each request will get a different response.
		actions[i] = &readRowsAction{
			chunks:      []chunkData{dummyChunkData(rowKeys[i], fmt.Sprintf("value%d", i), Commit)},
			delayStr:    "2s",
		}
	}
	server := initMockServer(t)
	server.ReadRowsFn = mockReadRowsFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.ReadRowRequest, concurrency) // Will be finished
	reqsBatchTwo := make([]*testproxypb.ReadRowRequest, concurrency) // Will be rejected by client
	for i := 0; i < concurrency; i++ {
		reqsBatchOne[i] = &testproxypb.ReadRowRequest{
			ClientId:  clientID,
			TableName: buildTableName("table"),
			RowKey:    rowKeys[i],
		}
		reqsBatchTwo[i] = &testproxypb.ReadRowRequest{
			ClientId:  clientID,
			TableName: buildTableName("table"),
			RowKey:    rowKeys[i + concurrency],
		}
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doReadRowOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doReadRowOpsCore(t, clientID, reqsBatchTwo, nil)

	// 4a. Check that server only receives batch-one requests
	assert.Equal(t, concurrency, len(recorder))

	// 4b. Check that all the batch-one requests succeeded
	checkResultOkStatus(t, resultsBatchOne...)
	for i := 0; i < concurrency; i++ {
		assert.Equal(t, rowKeys[i], string(resultsBatchOne[i].Row.Key))
	}

	// 4c. Check that all the batch-two requests failed at the proxy level:
	// the proxy tries to use close client. Client and server have nothing to blame.
	for i := 0; i < concurrency; i++ {
		assert.Empty(t, resultsBatchTwo[i])
	}
}

