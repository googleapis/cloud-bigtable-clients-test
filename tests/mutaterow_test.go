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
	"bytes"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// dummyMutateRowRequest returns a dummy MutateRowRequest. The number of mutations is customizable
// via `numMutations`. For simplicity, only "SetCell" is used, family names and timestamps are
// hard-coded.
func dummyMutateRowRequest(tableID string, rowKey []byte, numMutations int) *btpb.MutateRowRequest {
	req := &btpb.MutateRowRequest{
		TableName: buildTableName(tableID),
		RowKey: rowKey,
		Mutations: []*btpb.Mutation{},
	}
	for i := 0; i < numMutations; i++ {
		mutation := &btpb.Mutation{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{
					FamilyName:      "f",
					ColumnQualifier: []byte("col_" + strconv.Itoa(i)),
					TimestampMicros: 1000,
					Value:           []byte("value_" + strconv.Itoa(i)),
				},
			},
		}
		req.Mutations = append(req.Mutations, mutation)
	}
	return req
}

// TestMutateRow_NoRetry_NonprintableByteKey tests that client can specify non-printable byte strings as row key.
func TestMutateRow_NoRetry_NonprintableByteKey(t *testing.T) {
	// 1. Instantiate the mock function
	recorder := make(chan *mutateRowReqRecord, 1)
	action := &mutateRowAction{}
	mockFn := mockMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	// Set the nonprintable row key according to https://www.utf8-chartable.de/unicode-utf8-table.pl
	nonprintableByteKey, err := hex.DecodeString("13141516")
	if err != nil {
		t.Fatalf("Unable to convert hex to byte: %v", err)
	}
	req := testproxypb.MutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowRequest("table", nonprintableByteKey, 1),
	}

	// 3. Perform the operation via test proxy
	res := doMutateRowOp(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	loggedReq := <- recorder
	assert.Equal(t, 0, bytes.Compare(nonprintableByteKey, loggedReq.req.RowKey))
}

// TestMutateRow_NoRetry_MultipleMutations tests that client can specify multiple mutations for a row.
func TestMutateRow_NoRetry_MultipleMutations(t *testing.T) {
	// 1. Instantiate the mock function
	recorder := make(chan *mutateRowReqRecord, 1)
	action := &mutateRowAction{}
	mockFn := mockMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowRequest("table", []byte("row-01"), 2),
	}

	// 3. Perform the operation via test proxy
	res := doMutateRowOp(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	loggedReq := <- recorder
	assert.Equal(t, 2, len(loggedReq.req.Mutations))
}

// TestMutateRow_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestMutateRow_Generic_MultiStreams(t *testing.T) {
	// 0. Common variable
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row"}
	concurrency := len(rowKeys)
	const requestRecorderCapacity = 10

	// 1. Instantiate the mockserver function
	recorder := make(chan *mutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*mutateRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		// Each request will succeed after delay.
		actions[i] = &mutateRowAction{delayStr: "2s"}
	}
	mockFn := mockMutateRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.MutateRowRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		clientReq := dummyMutateRowRequest("table", []byte(rowKeys[i]), 1)
		reqs[i] = &testproxypb.MutateRowRequest{
			ClientId: t.Name(),
			Request:  clientReq,
		}
	}

	// 3. Perform the operations via test proxy
	results := doMutateRowOps(t, mockFn, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)
}
