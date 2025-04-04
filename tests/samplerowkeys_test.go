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
	"net/url"
	"strings"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestSampleRowKeys_Generic_Headers tests that SampleRowKeys request has client and resource
// info, as well as app_profile_id in the header.
func TestSampleRowKeys_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const profileID string = "test_profile"
	tableName := buildTableName("table")

	// 1. Instantiate the mock server
	// Don't call mockSampleRowKeysFn() as the behavior is to record metadata of the request
	mdRecords := make(chan metadata.MD, 1)
	server := initMockServer(t)
	server.SampleRowKeysFn = func(req *btpb.SampleRowKeysRequest, srv btpb.Bigtable_SampleRowKeysServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md
		return nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  &btpb.SampleRowKeysRequest{TableName: tableName},
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		profile: profileID,
	}
	doSampleRowKeysOp(t, server, &req, &opts)

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

// TestSampleRowKeys_NoRetry_NoEmptyKey tests that client should accept a list with no empty key.
func TestSampleRowKeys_NoRetry_NoEmptyKey(t *testing.T) {
	// 0. Common variables
	clientReq := &btpb.SampleRowKeysRequest{TableName: buildTableName("table")}

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, 1)
	sequence := []sampleRowKeysAction{
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30},
		sampleRowKeysAction{rowKey: []byte("row-98"), offsetBytes: 65},
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFn(recorder, sequence)

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doSampleRowKeysOp(t, server, &req, nil)

	// 4a. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.Equal(t, 2, len(res.GetSamples()))
	assert.Equal(t, "row-31", string(res.GetSamples()[0].RowKey))
	assert.Equal(t, "row-98", string(res.GetSamples()[1].RowKey))

	// 4b. Check that the request is received as expected
	loggedReq := <-recorder
	if diff := cmp.Diff(clientReq, loggedReq.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

// TestSampleRowKeys_Generic_MultiStreams tests that client can have multiple concurrent streams.
func TestSampleRowKeys_Generic_MultiStreams(t *testing.T) {
	// 0. Common variables
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
			ClientId: t.Name(),
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

// TestSampleRowKeys_Generic_CloseClient tests that client doesn't kill inflight requests after
// client closing, but will reject new requests.
func TestSampleRowKeys_Generic_CloseClient(t *testing.T) {
	// 0. Common variable
	halfBatchSize := 3
	clientID := t.Name()
	const requestRecorderCapacity = 10

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, requestRecorderCapacity)
	actions := make([]sampleRowKeysAction, 2*halfBatchSize)
	for i := 0; i < 2*halfBatchSize; i++ {
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
			Request:  &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
		}
		reqsBatchTwo[i] = &testproxypb.SampleRowKeysRequest{
			ClientId: clientID,
			Request:  &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
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

	// 4b. Check that all the batch-one requests succeeded or were cancelled
	checkResultOkOrCancelledStatus(t, resultsBatchOne...)

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

// TestSampleRowKeys_Generic_DeadlineExceeded tests that client-side timeout is set and respected.
func TestSampleRowKeys_Generic_DeadlineExceeded(t *testing.T) {
	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, 1)
	sequence := []sampleRowKeysAction{
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30, delayStr: "10s"},
		sampleRowKeysAction{rowKey: []byte("row-98"), offsetBytes: 65},
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFn(recorder, sequence)

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  &btpb.SampleRowKeysRequest{TableName: buildTableName("table")},
	}

	// 3. Perform the operation via test proxy
	opts := clientOpts{
		timeout: &durationpb.Duration{Seconds: 2},
	}
	res := doSampleRowKeysOp(t, server, &req, &opts)

	// 4a. Check the runtime
	curTs := time.Now()
	loggedReq := <-recorder
	runTimeSecs := int(curTs.Unix() - loggedReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8) // 8s (< 10s of server delay time) indicates timeout takes effect.

	// 4b. Check the DeadlineExceeded error
	assert.Equal(t, int32(codes.DeadlineExceeded), res.GetStatus().GetCode())
}

// TestSampleRowKeys_Retry_WithRoutingCookie tests that client handles routing cookie correctly.
func TestSampleRowKeys_Retry_WithRoutingCookie(t *testing.T) {
	// 0. Common variables
	cookie := "test-cookie"
	clientReq := &btpb.SampleRowKeysRequest{TableName: buildTableName("table")}

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, 2)
	mdRecorder := make(chan metadata.MD, 2)
	sequence := []sampleRowKeysAction{
		sampleRowKeysAction{rpcError: codes.Unavailable, routingCookie: cookie},
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30},
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFnWithMetadata(recorder, mdRecorder, sequence)

	// 2. Build the request to test proxy
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doSampleRowKeysOp(t, server, &req, nil)

	// 4a. Check that the operation succeeded
	checkResultOkStatus(t, res)
	assert.Equal(t, 1, len(res.GetSamples()))
	assert.Equal(t, "row-31", string(res.GetSamples()[0].RowKey))

	// 4b. Verify routing cookie is seen
	// Ignore the first metadata which won't have the routing cookie
	var _ = <-mdRecorder
	// second metadata which comes from the retry attempt should have a routing cookie field
	md1 := <-mdRecorder
	val := md1["x-goog-cbt-cookie-test"]
	assert.NotEmpty(t, val)
	if len(val) == 0 {
		return
	}
	assert.Equal(t, cookie, val[0])
}

// TestSampleRowKeys_Retry_WithRetryInfo tests that client handles RetryInfo correctly.
func TestSampleRowKeys_Retry_WithRetryInfo(t *testing.T) {

	// 1. Instantiate the mock server
	recorder := make(chan *sampleRowKeysReqRecord, 2)
	mdRecorder := make(chan metadata.MD, 2)
	sequence := []sampleRowKeysAction{
		sampleRowKeysAction{rpcError: codes.Unavailable, retryInfo: "2s"},
		sampleRowKeysAction{rowKey: []byte("row-31"), offsetBytes: 30},
	}
	server := initMockServer(t)
	server.SampleRowKeysFn = mockSampleRowKeysFnWithMetadata(recorder, mdRecorder, sequence)

	// 2. Build the request to test proxy
	clientReq := &btpb.SampleRowKeysRequest{TableName: buildTableName("table")}
	req := testproxypb.SampleRowKeysRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doSampleRowKeysOp(t, server, &req, nil)

	// 4a. Check that the overall operation succeeded
	checkResultOkStatus(t, res)

	// 4b. Verify retry backoff time is correct
	firstReq := <-recorder
	firstReqTs := firstReq.ts.Unix()

	select {
	case retryReq := <-recorder:
		retryReqTs := retryReq.ts.Unix()
		assert.True(t, retryReqTs-firstReqTs >= 2)
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for retry request")
	}
}
