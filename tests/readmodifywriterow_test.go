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

// +build !emulator

package tests

import (
	"context"
	"encoding/binary"
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

// dummyReadModifyWriteRowRequest returns a dummy ReadModifyWriteRowRequest.
// ReadModifyWriteRules are built from interleved "increment" and "append" actions.
// The columns are "f:col_str" and "f:col_int".
func dummyReadModifyWriteRowRequest(tableID string, rowKey []byte, increments []int64, appends []string) *btpb.ReadModifyWriteRowRequest {
	req := &btpb.ReadModifyWriteRowRequest{
		TableName: buildTableName(tableID),
		RowKey:    rowKey,
		Rules:	   []*btpb.ReadModifyWriteRule{},
	}

	i := 0
	ruleAdded := false
	for {
		if i < len(increments) {
			rule := &btpb.ReadModifyWriteRule{
				FamilyName:      "f",
				ColumnQualifier: []byte("col_int"),
				Rule:            &btpb.ReadModifyWriteRule_IncrementAmount{IncrementAmount: increments[i]},
			}
			req.Rules = append(req.Rules, rule)
			ruleAdded = true
		}
		if i < len(appends) {
			rule := &btpb.ReadModifyWriteRule{
				FamilyName:      "f",
				ColumnQualifier: []byte("col_str"),
				Rule:            &btpb.ReadModifyWriteRule_AppendValue{AppendValue: []byte(appends[i])},
			}
			req.Rules = append(req.Rules, rule)
			ruleAdded = true
		}
		if !ruleAdded {
			break
		}
		i += 1
		ruleAdded = false
	}
	return req
}

// dummyResultRow returns a row after applying ReadModifyWriteRules to a blank row.
// ReadModifyWriteRules are built from "increment" and "append" actions.
// The columns are f:col_str and f:col_int.
func dummyResultRow(rowKey []byte, increments []int64, appends []string) *btpb.Row {
	resultNum := int64(0)
	for _, num := range increments {
		resultNum += num
	}
	resultNumInBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(resultNumInBytes, uint64(resultNum))

	resultStr := ""
	for _, str := range appends {
		resultStr += str
	}

	row := &btpb.Row{
		Key:      rowKey,
		Families: []*btpb.Family{
			&btpb.Family{
				Name: "f",
				Columns: []*btpb.Column{
					&btpb.Column{
						Qualifier: []byte("col_int"),
						Cells:     []*btpb.Cell{&btpb.Cell{TimestampMicros: 2000, Value: resultNumInBytes}},
					},
					&btpb.Column{
						Qualifier: []byte("col_str"),
						Cells:     []*btpb.Cell{&btpb.Cell{TimestampMicros: 1000, Value: []byte(resultStr)}},
					},
				},
			},
		},
	}
	return row
}

// TestReadModifyWriteRow_Generic_Headers tests that ReadModifyWriteRow request has client and
// resource info, as well as app_profile_id in the header.
func TestReadModifyWriteRow_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const profileID string = "test_profile"
	const tableID string = "table"
	increments := []int64{10, 2}
	appends := []string{"str1", "str2"}
	rowKey := []byte("row-01")
	tableName := buildTableName(tableID)

	// 1. Instantiate the mock server
	// Don't call mockReadRowsFn() as the behavior is to record metadata of the request
	mdRecords := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = func(ctx context.Context, req *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mdRecords <- md

		return &btpb.ReadModifyWriteRowResponse{Row: dummyResultRow(rowKey, increments, appends)}, nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: t.Name(),
		Request: dummyReadModifyWriteRowRequest(tableID, rowKey, increments, appends),
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		profile: profileID,
	}
	doReadModifyWriteRowOp(t, server, &req, &opts)

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

// TestReadModifyWriteRow_NoRetry_MultiValues tests that client can increment & append multiple values.
func TestReadModifyWriteRow_NoRetry_MultiValues(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"str1", "str2"}
	rowKey := []byte("row-01")
	clientReq := dummyReadModifyWriteRowRequest("table", rowKey, increments, appends)

	// 1. Instantiate the mock server
	recorder := make(chan *readModifyWriteRowReqRecord, 1)
	action := &readModifyWriteRowAction{row: dummyResultRow(rowKey, increments, appends)}
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = mockReadModifyWriteRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doReadModifyWriteRowOp(t, server, &req, nil)

	// 4. Check that the dummy request is sent and the dummy row is returned
	checkResultOkStatus(t, res)
	loggedReq := <-recorder
	if diff := cmp.Diff(clientReq, loggedReq.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
	assert.Equal(t, rowKey, res.Row.Key)
	assert.Equal(t, 10 + 2, int(binary.BigEndian.Uint64(res.Row.Families[0].Columns[0].Cells[0].Value)))
	assert.Equal(t, "str1" + "str2", string(res.Row.Families[0].Columns[1].Cells[0].Value))
}

// TestReadModifyWriteRow_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestReadModifyWriteRow_Generic_MultiStreams(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"append"}
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row"}
	concurrency := len(rowKeys)
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *readModifyWriteRowReqRecord, requestRecorderCapacity)
	actions := make([]*readModifyWriteRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		actions[i] = &readModifyWriteRowAction{
			row: dummyResultRow([]byte(rowKeys[i]), increments, appends),
			delayStr: "2s"}
	}
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = mockReadModifyWriteRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqs := make([]*testproxypb.ReadModifyWriteRowRequest, concurrency)
	for i := 0; i < concurrency; i++ {
		clientReq := dummyReadModifyWriteRowRequest("table", []byte(rowKeys[i]), increments, appends)
		reqs[i] = &testproxypb.ReadModifyWriteRowRequest{
			ClientId: t.Name(),
			Request:  clientReq,
		}
	}

	// 3. Perform the operations via test proxy
	results := doReadModifyWriteRowOps(t, server, reqs, nil)

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

// TestReadModifyWriteRow_NoRetry_TransientError tests that client doesn't retry on transient errors.
func TestReadModifyWriteRow_NoRetry_TransientError(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"str1", "str2"}
	rowKey := []byte("row-01")
	clientReq := dummyReadModifyWriteRowRequest("table", rowKey, increments, appends)

	// 1. Instantiate the mock server
	records := make(chan *readModifyWriteRowReqRecord, 2)
	actions := []*readModifyWriteRowAction{
		&readModifyWriteRowAction{rpcError: codes.Unavailable},
		&readModifyWriteRowAction{row: dummyResultRow(rowKey, increments, appends)},
	}
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = mockReadModifyWriteRowFn(records, actions)

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doReadModifyWriteRowOp(t, server, &req, nil)

	// 4. Check that the result has error, and there is no retry
	assert.NotEmpty(t, res)
	assert.Equal(t, int32(codes.Unavailable), res.GetStatus().GetCode())
	assert.Equal(t, 1, len(records))
}

// TestReadModifyWriteRow_Generic_CloseClient tests that client doesn't kill inflight requests
// after client closing, but will reject new requests.
func TestReadModifyWriteRow_Generic_CloseClient(t *testing.T) {
	// 0. Common variable
	increments := []int64{10, 2}
	appends := []string{"append"}
	rowKeys := []string{"op0-row", "op1-row", "op2-row", "op3-row", "op4-row", "op5-row"}
	halfBatchSize := len(rowKeys) / 2
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *readModifyWriteRowReqRecord, requestRecorderCapacity)
	actions := make([]*readModifyWriteRowAction, 2 * halfBatchSize)
	for i := 0; i < 2 * halfBatchSize; i++ {
		actions[i] = &readModifyWriteRowAction{
			row: dummyResultRow([]byte(rowKeys[i]), increments, appends),
			delayStr: "2s",
		}
	}
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = mockReadModifyWriteRowFnSimple(recorder, actions...)

	// 2. Build the requests to test proxy
	reqsBatchOne := make([]*testproxypb.ReadModifyWriteRowRequest, halfBatchSize) // Will be finished
	reqsBatchTwo := make([]*testproxypb.ReadModifyWriteRowRequest, halfBatchSize) // Will be rejected by client
	for i := 0; i < halfBatchSize; i++ {
		reqsBatchOne[i] = &testproxypb.ReadModifyWriteRowRequest{
			ClientId: clientID,
			Request: dummyReadModifyWriteRowRequest("table", []byte(rowKeys[i]), increments, appends),
		}
		reqsBatchTwo[i] = &testproxypb.ReadModifyWriteRowRequest{
			ClientId:  clientID,
			Request: dummyReadModifyWriteRowRequest(
				"table", []byte(rowKeys[i + halfBatchSize]), increments, appends),
		}
	}

	// 3. Perform the operations via test proxy
	setUp(t, server, clientID, nil)
	defer tearDown(t, server, clientID)

	closeClientAfter := time.Second
	resultsBatchOne := doReadModifyWriteRowOpsCore(t, clientID, reqsBatchOne, &closeClientAfter)
	resultsBatchTwo := doReadModifyWriteRowOpsCore(t, clientID, reqsBatchTwo, nil)

	// 4a. Check that server only receives batch-one requests
	assert.Equal(t, halfBatchSize, len(recorder))

	// 4b. Check that all the batch-one requests succeeded
	checkResultOkStatus(t, resultsBatchOne...)
	for i := 0; i < halfBatchSize; i++ {
		assert.Equal(t, rowKeys[i], string(resultsBatchOne[i].Row.Key))
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

// TestReadModifyWriteRow_Generic_DeadlineExceeded tests that client-side timeout is set and
// respected.
func TestReadModifyWriteRow_Generic_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"str1", "str2"}
	rowKey := []byte("row-01")
	clientReq := dummyReadModifyWriteRowRequest("table", rowKey, increments, appends)

	// 1. Instantiate the mock server
	recorder := make(chan *readModifyWriteRowReqRecord, 1)
	action := &readModifyWriteRowAction{
		row: dummyResultRow(rowKey, increments, appends),
		delayStr: "10s",
	}
	server := initMockServer(t)
	server.ReadModifyWriteRowFn = mockReadModifyWriteRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: t.Name(),
		Request: clientReq,
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		timeout: &durationpb.Duration{Seconds: 2},
	}
	res := doReadModifyWriteRowOp(t, server, &req, &opts)

	// 4a. Check the runtime
	curTs := time.Now()
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8) // 8s (< 10s of server delay time) indicates timeout takes effect.

	// 4b. Check the DeadlineExceeded error
	assert.Equal(t, int32(codes.DeadlineExceeded), res.GetStatus().GetCode())
}

