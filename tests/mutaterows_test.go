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
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// buildEntryData returns an instance of entryData type based on the mutated and failed rows.
// Row indices are used: all the rows with `mutatedRowIndices` are mutated successfully;
// and all the rows with `failedRowIndices` have failures with `errorCode`.
// The function will check if non-OK `errorCode` is passed in when there are failed rows.
func buildEntryData(mutatedRowIndices []int, failedRowIndices []int, errorCode codes.Code) entryData {
	result := entryData{}
	if len(mutatedRowIndices) > 0 {
		result.mutatedRows = mutatedRowIndices
	}
	if len(failedRowIndices) > 0 {
		if errorCode == codes.OK {
			log.Fatal("errorCode should be non-OK")
		}
		result.failedRows = map[codes.Code][]int{errorCode: failedRowIndices}
	}
	return result
}

// dummyMutateRowsRequestCore returns a dummy MutateRowsRequest for the given table and rowkeys.
// For simplicity, only one "SetCell" mutation is used for each row; family & column names, values,
// and timestamps are hard-coded.
func dummyMutateRowsRequestCore(tableID string, rowKeys []string) *btpb.MutateRowsRequest {
	req := &btpb.MutateRowsRequest{
		TableName: buildTableName(tableID),
		Entries: []*btpb.MutateRowsRequest_Entry{},
	}
	for i := 0; i < len(rowKeys); i++ {
		entry := &btpb.MutateRowsRequest_Entry{
			RowKey: []byte(rowKeys[i]),
			Mutations: []*btpb.Mutation{
				{Mutation: &btpb.Mutation_SetCell_{
					SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "f",
						ColumnQualifier: []byte("col"),
						TimestampMicros: 1000,
						Value:           []byte("value"),
					},
				}},
			},
		}
		req.Entries = append(req.Entries, entry)
	}
	return req
}

// dummyMutateRowsRequest returns a dummy MutateRowsRequest for the given table and row count.
// rowkeys and values are generated with the row indices.
func dummyMutateRowsRequest(tableID string, numRows int) *btpb.MutateRowsRequest {
	rowKeys := []string{}
	for i := 0; i < numRows; i++ {
		rowKeys = append(rowKeys, "row-" + strconv.Itoa(i))
	}
	return dummyMutateRowsRequestCore(tableID, rowKeys)
}

// TestMutateRows_Generic_Headers tests that MutateRows request has client and resource info in the
// header.
func TestMutateRows_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const numRows int = 2
	const tableID string = "table"

	// 1. Instantiate the mock function
	// Don't call mockMutateRowsFn() as the behavior is to record metadata of the request.
	mdRecords := make(chan metadata.MD, 1)
	mockFn := func(req *btpb.MutateRowsRequest, srv btpb.Bigtable_MutateRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md

		// C++ client requires per-row result to be set, otherwise the client returns Internal error.
		// For Java client, using "return nil" is enough.
		res := &btpb.MutateRowsResponse{}
		for i := 0; i < numRows; i++ {
			res.Entries = append(res.Entries, &btpb.MutateRowsResponse_Entry{
				Index:  int64(i),
				Status: &status.Status{},
			})
		}
		return srv.Send(res)
	}

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowsRequest(tableID, numRows),
	}

	// 3. Perform the operation via test proxy
	doMutateRowsOp(t, mockFn, &req, nil)

	// 4. Check the request headers in the metadata
	md := <-mdRecords
	assert.NotEmpty(t, md["x-goog-api-client"])
	assert.Contains(t, md["x-goog-request-params"][0], buildTableName(tableID))
}

// TestMutateRows_NoRetry_NonTransientErrors tests that client will not retry on non-transient errors.
func TestMutateRows_NoRetry_NonTransientErrors(t *testing.T) {
	// 0. Common variables
	const numRows int = 4
	const numRPCs int = 1
	const tableID string = "table"
	mutatedRowIndices := []int{0, 3}
	failedRowIndices := []int{1, 2}

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numRPCs + 1)
	action := mutateRowsAction{ // There are 4 rows to mutate, row-1 and row-2 have errors.
		data: buildEntryData(mutatedRowIndices, failedRowIndices, codes.PermissionDenied),
	}
	mockFn := mockMutateRowsFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowsRequest(tableID, numRows),
	}

	// 3. Perform the operation via test proxy
	res := doMutateRowsOp(t, mockFn, &req, nil)

	// 4a. Check the number of requests in the records
	assert.Equal(t, numRPCs, len(records))

	// 4b. Check the per-row status
	assert.Empty(t, res.GetStatus().GetCode())
	outputIndices := []int{}
	for _, entry := range res.GetEntry() {
		outputIndices = append(outputIndices, int(entry.GetIndex()))
		assert.Equal(t, int32(codes.PermissionDenied), entry.GetStatus().GetCode())
	}
	assert.ElementsMatch(t, failedRowIndices, outputIndices)
}

// TestMutateRows_NoRetry_DeadlineExceeded tests that deadline is set correctly.
func TestMutateRows_NoRetry_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	const numRows int = 1
	const numRPCs int = 1
	const tableID string = "table"

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numRPCs + 1)
	action := mutateRowsAction{ // There is one row to mutate, which has a long delay.
		data: buildEntryData([]int{0}, nil, 0),
		delayStr: "10s",
	}
	mockFn := mockMutateRowsFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowsRequest(tableID, numRows),
	}

	// 3. Perform the operation via test proxy
	timeout := durationpb.Duration{
		Seconds: 2,
	}
	res := doMutateRowsOp(t, mockFn, &req, &timeout)
	curTs := time.Now()

	// 4a. Check the number of requests in the records
	assert.Equal(t, numRPCs, len(records))

	// 4b. Check the runtime
	origReq := <-records
	runTimeSecs := int(curTs.Unix() - origReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 8) // 8s (< 10s of server delay time) indicates timeout takes effect.

	// 4c. Check the per-row error
	assert.Empty(t, res.GetStatus().GetCode())
	for _, entry := range res.GetEntry() {
		assert.Equal(t, int32(codes.DeadlineExceeded), entry.GetStatus().GetCode())
	}
}

// TestMutateRows_Retry_TransientErrors tests that client will retry transient errors.
func TestMutateRows_Retry_TransientErrors(t *testing.T) {
	// 0. Common variables
	const numRows int = 4
	const numRPCs int = 3
	const tableID string = "table"
	clientReq := dummyMutateRowsRequest(tableID, numRows)

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numRPCs + 1)
	actions := []mutateRowsAction{
		mutateRowsAction{ // There are 4 rows to mutate, row-1 and row-2 have errors.
			data:        buildEntryData([]int{0, 3}, []int{1, 2}, codes.Unavailable),
			endOfStream: true,
		},
		mutateRowsAction{ // Retry for the two failed rows, row-1 has error.
			data:        buildEntryData([]int{1}, []int{0}, codes.Unavailable),
			endOfStream: true,
		},
		mutateRowsAction{ // Retry for the one failed row, which has no error.
			data: buildEntryData([]int{0}, nil, 0),
		},
	}
	mockFn := mockMutateRowsFn(records, actions...)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doMutateRowsOp(t, mockFn, &req, nil)

	// 4a. Check that the overall operation succeeded
	assert.Empty(t, res.GetStatus().GetCode())

	// 4b. Check the number of requests in the records
	assert.Equal(t, numRPCs, len(records))

	// 4c. Check the recorded requests
	origReq := <-records
	firstRetry := <-records
	secondRetry := <-records

	if diff := cmp.Diff(clientReq, origReq.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}

	expectedFirstRetry := dummyMutateRowsRequestCore(tableID, []string{"row-1", "row-2"})
	if diff := cmp.Diff(expectedFirstRetry, firstRetry.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}

	expectedSecondRetry := dummyMutateRowsRequestCore(tableID, []string{"row-1"})
	if diff := cmp.Diff(expectedSecondRetry, secondRetry.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
}

// TestMutateRows_RetryClientGap_ExponentialBackoff tests that client will retry using exponential backoff.
// Not all client libraries satisfy the requirement, so "ClientGap" is added to the name.
func TestMutateRows_RetryClientGap_ExponentialBackoff(t *testing.T) {
	// 0. Common variables
	const numRows int = 1
	const numRPCs int = 4
	const tableID string = "table"

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numRPCs + 1)
	actions := []mutateRowsAction{
		mutateRowsAction{ // There is one row to mutate, which has error.
			data:        buildEntryData(nil, []int{0}, codes.Unavailable),
			endOfStream: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has error.
			data:        buildEntryData(nil, []int{0}, codes.Unavailable),
			endOfStream: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has error.
			data:        buildEntryData(nil, []int{0}, codes.Unavailable),
			endOfStream: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has no error.
			data: buildEntryData([]int{0}, nil, 0),
		},
	}
	mockFn := mockMutateRowsFn(records, actions...)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: t.Name(),
		Request:  dummyMutateRowsRequest(tableID, numRows),
	}

	// 3. Perform the operation via test proxy
	doMutateRowsOp(t, mockFn, &req, nil)

	// 4a. Check the number of requests in the records
	assert.Equal(t, numRPCs, len(records))

	// 4b. Check the retry delays
	origReq := <-records
	firstRetry := <-records
	secondRetry := <-records
	thirdRetry := <-records

	firstDelay := int(firstRetry.ts.UnixMilli() - origReq.ts.UnixMilli())
	secondDelay := int(secondRetry.ts.UnixMilli() - firstRetry.ts.UnixMilli())
	thirdDelay := int(thirdRetry.ts.UnixMilli() - secondRetry.ts.UnixMilli())

	// Different clients may have different behaviors, we log the delays for informational purpose.
	// Example: For the first retry delay, C++ client uses 100ms but Java client uses 10ms.
	// Java client allows the second delay to be smaller than the first delay but C++ client doesn't.
	t.Logf("The three retry delays are: %dms, %dms, %dms", firstDelay, secondDelay, thirdDelay)

	// Basic assertions are used, but not all clients can pass them consistently.
	assert.Less(t, firstDelay, secondDelay)
	assert.Less(t, secondDelay, thirdDelay)
}
