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

// dummyCheckAndMutateRowRequest returns a dummy CheckAndMutateRowRequest. For simplicity,
// predicateFilter is not set, and either TrueMutations or FalseMutations can be set (but not both).
func dummyCheckAndMutateRowRequest(tableID string, rowKey []byte, predicateMatched bool, numMutations int) *btpb.CheckAndMutateRowRequest {
	req := &btpb.CheckAndMutateRowRequest{
		TableName:      buildTableName(tableID),
		RowKey:         rowKey,
		TrueMutations:  []*btpb.Mutation{},
		FalseMutations: []*btpb.Mutation{},
	}

	for i := 0; i < numMutations; i++ {
		mutation := &btpb.Mutation{
			Mutation: &btpb.Mutation_SetCell_{
				SetCell: &btpb.Mutation_SetCell{},
			},
		}
		if predicateMatched {
			req.TrueMutations = append(req.TrueMutations, mutation)
		} else {
			req.FalseMutations = append(req.FalseMutations, mutation)
		}
	}

	return req
}

// TestCheckAndMutateRow_NoRetry_TrueMutations tests that client can request true mutations.
func TestCheckAndMutateRow_NoRetry_TrueMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = true
	rowKey := []byte("row-01")

	// 1. Instantiate the mock function
	recorder := make(chan *checkAndMutateRowReqRecord, 1)
	action := &checkAndMutateRowAction{predicateMatched: predicateMatched}
	mockFn := mockCheckAndMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest("table", rowKey, predicateMatched, 2),
	}

	// 3. Perform the operation via test proxy
	res := doCheckAndMutateRowOp(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.True(t, res.Result.PredicateMatched)
	loggedReq := <- recorder
	assert.Equal(t, 2, len(loggedReq.req.TrueMutations))
	assert.Empty(t, loggedReq.req.FalseMutations)
}

// TestCheckAndMutateRow_NoRetry_FalseMutations tests that client can request false mutations.
func TestCheckAndMutateRow_NoRetry_FalseMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = false
	rowKey := []byte("row-01")

	// 1. Instantiate the mock function
	recorder := make(chan *checkAndMutateRowReqRecord, 1)
	action := &checkAndMutateRowAction{predicateMatched: predicateMatched}
	mockFn := mockCheckAndMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest("table", rowKey, predicateMatched, 2),
	}

	// 3. Perform the operation via test proxy
	res := doCheckAndMutateRowOp(t, mockFn, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.False(t, res.Result.PredicateMatched)
	loggedReq := <- recorder
	assert.Equal(t, 2, len(loggedReq.req.FalseMutations))
	assert.Empty(t, loggedReq.req.TrueMutations)
}

// TestCheckAndMutateRow_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestCheckAndMutateRow_Generic_MultiStreams(t *testing.T) {
	// 0. Common variable
	predicateMatched := []bool{false, true, true, false, false}
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row"}
	concurrency := len(rowKeys)
	const requestRecorderCapacity = 10

	// 1. Instantiate the mockserver function
	recorder := make(chan *checkAndMutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*checkAndMutateRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		actions[i] = &checkAndMutateRowAction{
			predicateMatched: predicateMatched[i],
			delayStr: "2s",
		}
	}
	mockFn := mockCheckAndMutateRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.CheckAndMutateRowRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		clientReq := dummyCheckAndMutateRowRequest("table", []byte(rowKeys[i]), predicateMatched[i], 1)
		reqs[i] = &testproxypb.CheckAndMutateRowRequest{
			ClientId: t.Name(),
			Request:  clientReq,
		}
	}

	// 3. Perform the operations via test proxy
	results := doCheckAndMutateRowOps(t, mockFn, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)

	// 4c. Check the results.
	for i := 0; i < concurrency; i++ {
		assert.Equal(t, predicateMatched[i], results[i].Result.PredicateMatched)
	}
}
