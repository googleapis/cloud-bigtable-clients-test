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

//go:build !emulator
// +build !emulator

package tests

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
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

// TestCheckAndMutateRow_Generic_Headers tests that CheckAndMutateRow request has client and
// resource info, as well as app_profile_id in the header.
func TestCheckAndMutateRow_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const predicateMatched bool = true
	const profileID string = "test_profile"
	const tableID string = "table"
	rowKey := []byte("row-01")
	tableName := buildTableName(tableID)

	// 1. Instantiate the mock server
	// Don't call mockCheckAndMutateRowFn() as the behavior is to record metadata of the request
	mdRecords := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.CheckAndMutateRowFn = func(ctx context.Context, req *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mdRecords <- md

		return &btpb.CheckAndMutateRowResponse{PredicateMatched: predicateMatched}, nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest(tableID, rowKey, predicateMatched, 1),
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		profile: profileID,
	}
	doCheckAndMutateRowOp(t, server, &req, &opts)

	// 4. Check the request headers in the metadata
	md := <-mdRecords
	if len(md["user-agent"]) == 0 && len(md["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}

	resource := md["x-goog-request-params"][0]
	if !strings.Contains(resource, tableName) && !strings.Contains(resource, url.QueryEscape(tableName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, resource, profileID)
}

// TestCheckAndMutateRow_NoRetry_TrueMutations tests that client can request true mutations.
func TestCheckAndMutateRow_NoRetry_TrueMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = true
	rowKey := []byte("row-01")
	clientReq := dummyCheckAndMutateRowRequest("table", rowKey, predicateMatched, 2)

	// 1. Instantiate the mock server
	recorder := make(chan *checkAndMutateRowReqRecord, 1)
	action := &checkAndMutateRowAction{predicateMatched: predicateMatched}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doCheckAndMutateRowOp(t, server, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.True(t, res.Result.PredicateMatched)
	loggedReq := <-recorder
	assert.Equal(t, 2, len(loggedReq.req.TrueMutations))
	assert.Empty(t, loggedReq.req.FalseMutations)
	if diff := cmp.Diff(clientReq, loggedReq.req, protocmp.Transform(), protocmp.IgnoreEmptyMessages()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

// TestCheckAndMutateRow_NoRetry_FalseMutations tests that client can request false mutations.
func TestCheckAndMutateRow_NoRetry_FalseMutations(t *testing.T) {
	// 0. Common variable
	const predicateMatched bool = false
	rowKey := []byte("row-01")

	// 1. Instantiate the mock server
	recorder := make(chan *checkAndMutateRowReqRecord, 1)
	action := &checkAndMutateRowAction{predicateMatched: predicateMatched}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest("table", rowKey, predicateMatched, 2),
	}

	// 3. Perform the operation via test proxy
	res := doCheckAndMutateRowOp(t, server, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.False(t, res.Result.PredicateMatched)
	loggedReq := <-recorder
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

	// 1. Instantiate the mock server
	recorder := make(chan *checkAndMutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*checkAndMutateRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		actions[i] = &checkAndMutateRowAction{
			predicateMatched: predicateMatched[i],
			delayStr:         "2s",
		}
	}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFnSimple(recorder, actions...)

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
	results := doCheckAndMutateRowOps(t, server, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)

	// 4c. Check the results.
	for i := 0; i < concurrency; i++ {
		assert.NotNil(t, results[i])
		if results[i] == nil {
			continue
		}
		assert.NotNil(t, results[i].Result)
		if results[i].Result == nil {
			continue
		}
		assert.Equal(t, predicateMatched[i], results[i].Result.PredicateMatched)
	}
}

// TestCheckAndMutateRow_NoRetry_TransientError tests that client doesn't retry on transient errors.
func TestCheckAndMutateRow_NoRetry_TransientError(t *testing.T) {
	// 0. Common variables
	const predicateMatched bool = false
	rowKey := []byte("row-01")

	// 1. Instantiate the mock server
	records := make(chan *checkAndMutateRowReqRecord, 2)
	actions := []*checkAndMutateRowAction{
		&checkAndMutateRowAction{rpcError: codes.Unavailable},
		&checkAndMutateRowAction{predicateMatched: predicateMatched},
	}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFn(records, actions)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest("table", rowKey, predicateMatched, 2),
	}

	// 3. Perform the operation via test proxy
	res := doCheckAndMutateRowOp(t, server, &req, nil)

	// 4. Check that the result has error, and there is no retry
	assert.NotEmpty(t, res)
	assert.Equal(t, int32(codes.Unavailable), res.GetStatus().GetCode())
	assert.Equal(t, 1, len(records))
}

// TestCheckAndMutateRow_Generic_CloseClient tests that client doesn't kill inflight
// requests after client closing, but will reject new requests.
func TestCheckAndMutateRow_Generic_CloseClient(t *testing.T) {
	// 0. Common variable
	predicateMatched := []bool{false, true, true, false, false, true}
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row", "op5-row"}
	halfBatchSize := len(rowKeys) / 2
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *checkAndMutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*checkAndMutateRowAction, 2*halfBatchSize)
	for i := 0; i < 2*halfBatchSize; i++ {
		// Each request will get a different response.
		actions[i] = &checkAndMutateRowAction{
			predicateMatched: predicateMatched[i],
			delayStr:         "2s",
		}
	}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.CheckAndMutateRowRequest, halfBatchSize) // Will be finished
	reqsBatchTwo := make([]*testproxypb.CheckAndMutateRowRequest, halfBatchSize) // Will be rejected by client
	for i := 0; i < halfBatchSize; i++ {
		reqsBatchOne[i] = &testproxypb.CheckAndMutateRowRequest{
			ClientId: clientID,
			Request:  dummyCheckAndMutateRowRequest("table", []byte(rowKeys[i]), predicateMatched[i], 1),
		}
		reqsBatchTwo[i] = &testproxypb.CheckAndMutateRowRequest{
			ClientId: clientID,
			Request: dummyCheckAndMutateRowRequest(
				"table", []byte(rowKeys[i+halfBatchSize]), predicateMatched[i+halfBatchSize], 1),
		}
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doCheckAndMutateRowOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doCheckAndMutateRowOpsCore(t, clientID, reqsBatchTwo, nil)

	// 4a. Check that server only receives batch-one requests
	assert.Equal(t, halfBatchSize, len(recorder))

	// 4b. Check that all the batch-one requests succeeded or were cancelled
	checkResultOkOrCancelledStatus(t, resultsBatchOne...)
	for i := 0; i < halfBatchSize; i++ {
		resCode := resultsBatchOne[i].GetStatus().GetCode()
		if resCode == int32(codes.Canceled) {
			continue
		}
		assert.NotNil(t, resultsBatchOne[i].Result)
		if resultsBatchOne[i].Result == nil {
			continue
		}
		assert.Equal(t, predicateMatched[i], resultsBatchOne[i].Result.PredicateMatched)
	}

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

// TestCheckAndMutateRow_Generic_DeadlineExceeded tests that client-side timeout is set and
// respected.
func TestCheckAndMutateRow_Generic_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	const predicateMatched bool = true

	// 1. Instantiate the mock server
	recorder := make(chan *checkAndMutateRowReqRecord, 1)
	action := &checkAndMutateRowAction{
		predicateMatched: predicateMatched,
		delayStr:         "10s",
	}
	server := initMockServer(t)
	server.CheckAndMutateRowFn = mockCheckAndMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.CheckAndMutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyCheckAndMutateRowRequest("table", []byte("row-01"), predicateMatched, 2),
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		timeout: &durationpb.Duration{Seconds: 2},
	}
	res := doCheckAndMutateRowOp(t, server, &req, &opts)
	curTs := time.Now()

	// 4a. Check the runtime
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8) // 8s (< 10s of server delay time) indicates timeout takes effect.

	// 4b. Check the DeadlineExceeded error
	assert.Equal(t, int32(codes.DeadlineExceeded), res.GetStatus().GetCode())
}
