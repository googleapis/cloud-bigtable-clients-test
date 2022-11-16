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
	"context"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
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

// TestMutateRow_Generic_Headers tests that MutateRow request has client and resource info, as well
// as app_profile_id in the header.
func TestMutateRow_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const tableID string = "table"
	tableName := buildTableName(tableID)

	// 1. Instantiate the mock server
	// Don't call mockMutateRowFn() as the behavior is to record metadata of the request
	mdRecords := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.MutateRowFn = func(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mdRecords <- md

		return &btpb.MutateRowResponse{}, nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowRequest(tableID, []byte("row-01"), 1),
	}

	// 3. Perform the operation via test proxy
	doMutateRowOp(t, server, &req, nil)

	// 4. Check the request headers in the metadata
	md := <-mdRecords
	if len(md["user-agent"]) == 0 && len(md["x-goog-api-client"]) == 0 {
		assert.Fail(t, "Client info is missing in the request header")
	}

	resource := md["x-goog-request-params"][0]
        if !strings.Contains(resource, tableName) && !strings.Contains(resource, url.QueryEscape(tableName)) {
		assert.Fail(t, "Resource info is missing in the request header")
	}
	assert.Contains(t, resource, "app_profile_id=")
}

// TestMutateRow_NoRetry_NonprintableByteKey tests that client can specify non-printable byte strings as row key.
func TestMutateRow_NoRetry_NonprintableByteKey(t *testing.T) {
	// 1. Instantiate the mock server
	recorder := make(chan *mutateRowReqRecord, 1)
	action := &mutateRowAction{}
	server := initMockServer(t)
	server.MutateRowFn = mockMutateRowFnSimple(recorder, action)

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
	res := doMutateRowOp(t, server, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	loggedReq := <-recorder
	assert.Equal(t, 0, bytes.Compare(nonprintableByteKey, loggedReq.req.RowKey))
}

// TestMutateRow_NoRetry_MultipleMutations tests that client can specify multiple mutations for a row.
func TestMutateRow_NoRetry_MultipleMutations(t *testing.T) {
	// 0. Common variables
	clientReq := dummyMutateRowRequest("table", []byte("row-01"), 2)

	// 1. Instantiate the mock server
	recorder := make(chan *mutateRowReqRecord, 1)
	action := &mutateRowAction{}
	server := initMockServer(t)
	server.MutateRowFn = mockMutateRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowRequest{
		ClientId: t.Name(),
		Request: clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doMutateRowOp(t, server, &req, nil)

	// 4. Check that the operation succeeded
	checkResultOkStatus(t, res)
	loggedReq := <-recorder
	assert.Equal(t, 2, len(loggedReq.req.Mutations))
	if diff := cmp.Diff(clientReq, loggedReq.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

// TestMutateRow_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestMutateRow_Generic_MultiStreams(t *testing.T) {
	// 0. Common variables
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row"}
	concurrency := len(rowKeys)
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *mutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*mutateRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		// Each request will succeed after delay.
		actions[i] = &mutateRowAction{delayStr: "2s"}
	}
	server := initMockServer(t)
	server.MutateRowFn = mockMutateRowFnSimple(recorder, actions...)

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
	results := doMutateRowOps(t, server, reqs, nil)

	// 4a. Check that all the requests succeeded
	assert.Equal(t, concurrency, len(results))
	checkResultOkStatus(t, results...)

	// 4b. Check that the timestamps of requests should be very close
	assert.Equal(t, concurrency, len(recorder))
	checkRequestsAreWithin(t, 1000, recorder)
}

// TestMutateRow_GenericClientGap_CloseClient tests that client doesn't kill inflight requests
// after client closing, but will reject new requests.
func TestMutateRow_GenericClientGap_CloseClient(t *testing.T) {
	// 0. Common variable
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row", "op5-row"}
	halfBatchSize := len(rowKeys) / 2
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *mutateRowReqRecord, requestRecorderCapacity)
	actions := make([]*mutateRowAction, 2 * halfBatchSize)
	for i := 0; i < 2 * halfBatchSize; i++ {
		actions[i] = &mutateRowAction{delayStr: "2s"}
	}
	server := initMockServer(t)
	server.MutateRowFn = mockMutateRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.MutateRowRequest, halfBatchSize) // Will be finished
	reqsBatchTwo := make([]*testproxypb.MutateRowRequest, halfBatchSize) // Will be rejected by client
	for i := 0; i < halfBatchSize; i++ {
		reqsBatchOne[i] = &testproxypb.MutateRowRequest{
			ClientId: clientID,
			Request: dummyMutateRowRequest("table", []byte(rowKeys[i]), 1),
		}
		reqsBatchTwo[i] = &testproxypb.MutateRowRequest{
			ClientId: clientID,
			Request: dummyMutateRowRequest("table", []byte(rowKeys[i + halfBatchSize]), 1),
		}
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doMutateRowOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doMutateRowOpsCore(t, clientID, reqsBatchTwo, nil)

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

