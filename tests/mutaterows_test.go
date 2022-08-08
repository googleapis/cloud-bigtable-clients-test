// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// buildSimpleEntries builds and returns a simple instance of entryData type.
// The result indicates that all the rows with `mutatedRowIndices` are mutated successfully,
// and all the rows with `failedRowIndices` have failures with `errorCode`.
// The function will check if non-OK `errorCode` is passed in when there are failed rows.
func buildSimpleEntries(mutatedRowIndices []int, failedRowIndices []int, errorCode codes.Code) entryData {
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

// dummyMutateRowsRequestCore returns a dummy MutateRowsRequest for the given rowkeys and values.
// For simplicity, only one "SetCell" mutation is used for each row; family & column names and
// timestamp are fixed.
func dummyMutateRowsRequestCore(rowKeys []string, values []string) *btpb.MutateRowsRequest {
	if len(rowKeys) != len(values) {
		log.Fatal("rowKeys and values should have the same length")
	}
	numOfRows := len(rowKeys)

	req := &btpb.MutateRowsRequest{TableName: tableName, Entries: []*btpb.MutateRowsRequest_Entry{}}
	for i := 0; i < numOfRows; i++ {
		entry := &btpb.MutateRowsRequest_Entry{
			RowKey: []byte(rowKeys[i]),
			Mutations: []*btpb.Mutation{
				{Mutation: &btpb.Mutation_SetCell_{
					SetCell: &btpb.Mutation_SetCell{
						FamilyName:      "f",
						ColumnQualifier: []byte("col"),
						TimestampMicros: 1000,
						Value:           []byte(values[i]),
					},
				}},
			},
		}
		req.Entries = append(req.Entries, entry)
	}

	return req
}

// dummyMutateRowsRequest returns a dummy MutateRowsRequest for the given number of rows.
// rowkeys and values are generated with the row indices.
func dummyMutateRowsRequest(numOfRows int) *btpb.MutateRowsRequest {
	rowKeys := []string{}
	values := []string{}
	for i := 0; i < numOfRows; i++ {
		rowKeys = append(rowKeys, "row-" + strconv.Itoa(i))
		values = append(values, "value_" + strconv.Itoa(i))
	}
	return dummyMutateRowsRequestCore(rowKeys, values)
}

// TestMutateRows_Generic_Headers tests that MutateRows request has client and resource info in the
// header.
func TestMutateRows_Generic_Headers(t *testing.T) {
	// 0. Common variables
	const numOfRows int = 2

	// 1. Instantiate the mock function
	// Don't call mockMutateRowsFn() as the behavior is to record metadata of the request.
	mdRecords := make(chan metadata.MD, 1)
	mockFn := func(req *btpb.MutateRowsRequest, srv btpb.Bigtable_MutateRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md

		// C++ client requires per-row result to be set, otherwise the client returns Internal error.
		// For Java client, using "return nil" is enough.
		res := &btpb.MutateRowsResponse{}
		for i := 0; i < numOfRows; i++ {
			res.Entries = append(res.Entries, &btpb.MutateRowsResponse_Entry{
				Index:  int64(i),
				Status: &status.Status{},
			})
		}
		return srv.Send(res)
	}

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: "TestMutateRows_Generic_Headers",
		Request:  dummyMutateRowsRequest(numOfRows),
	}

	// 3. Conduct the test
	runMutateRowsTest(t, mockFn, &req, nil)

	// 4. Check the request headers in the metadata
	md := <-mdRecords
	assert.NotEmpty(t, md["x-goog-api-client"])
	assert.Contains(t, md["x-goog-request-params"][0], tableName)
}

// TestMutateRows_Basic_NonTransientErrors tests that client will not retry on non-transient errors.
func TestMutateRows_Basic_NonTransientErrors(t *testing.T) {
	// 0. Common variables
	const numOfRows int = 4
	const numOfRPCs int = 1
	mutatedRowIndices := []int{0, 3}
	failedRowIndices := []int{1, 2}

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numOfRPCs + 1)
	action := mutateRowsAction{ // There are 4 rows to mutate, row-1 and row-2 have errors.
		entries:   buildSimpleEntries(
			mutatedRowIndices, failedRowIndices, codes.PermissionDenied),
	}
	mockFn := mockMutateRowsFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: "TestMutateRows_Basic_NonTransientErrors",
		Request:  dummyMutateRowsRequest(numOfRows),
	}

	// 3. Conduct the test
	res := runMutateRowsTest(t, mockFn, &req, nil)

	// 4a. Check the number of requests in the records
	assert.Equal(t, numOfRPCs, len(records))

	// 4b. Check the per-row status
	assert.Empty(t, res.GetStatus().GetCode())
	outputIndices := []int{}
	for _, entry := range res.GetEntry() {
		outputIndices = append(outputIndices, int(entry.GetIndex()))
		assert.Equal(t, int32(codes.PermissionDenied), entry.GetStatus().GetCode())
	}
	assert.ElementsMatch(t, failedRowIndices, outputIndices)
}

// TestMutateRows_Basic_DeadlineExceeded tests that deadline is set correctly.
func TestMutateRows_Basic_DeadlineExceeded(t *testing.T) {
	// 0. Common variables
	const numOfRows int = 1
	const numOfRPCs int = 1

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numOfRPCs + 1)
	action := mutateRowsAction{ // There is one row to mutate, which has a long delay.
		entries: buildSimpleEntries([]int{0}, nil, 0),
		delayStr: "5s",
	}
	mockFn := mockMutateRowsFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: "TestMutateRows_Basic_DeadlineExceeded",
		Request:  dummyMutateRowsRequest(numOfRows),
	}

	// 3. Conduct the test
	timeout := durationpb.Duration{
		Seconds: 2, // The value is smaller than the default InitialRpcTimeout, so there won't be retry
	}
	res := runMutateRowsTest(t, mockFn, &req, &timeout)
	curTs := time.Now()

	// 4a. Check the number of requests in the records
	assert.Equal(t, numOfRPCs, len(records))

	// 4b. Check the runtime
	origReq := <-records
	runTimeSecs := int(curTs.Unix() - origReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 5)

	// 4c. Check the per-row error
	assert.Empty(t, res.GetStatus().GetCode())
	for _, entry := range res.GetEntry() {
		assert.Equal(t, int32(codes.DeadlineExceeded), entry.GetStatus().GetCode())
	}
}

// TestMutateRows_Retry_TransientErrors tests that client will retry transient errors.
func TestMutateRows_Retry_TransientErrors(t *testing.T) {
	// 0. Common variables
	const numOfRows int = 4
	const numOfRPCs int = 3

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numOfRPCs + 1)
	actions := []mutateRowsAction{
		mutateRowsAction{ // There are 4 rows to mutate, row-1 and row-2 have errors.
			entries:   buildSimpleEntries([]int{0, 3}, []int{1, 2}, codes.Unavailable),
			isLastRes: true,
		},
		mutateRowsAction{ // Retry for the two failed rows, row-1 has error.
			entries:   buildSimpleEntries([]int{1}, []int{0}, codes.Unavailable),
			isLastRes: true,
		},
		mutateRowsAction{ // Retry for the one failed row, which has no error.
			entries: buildSimpleEntries([]int{0}, nil, 0),
		},
	}
	mockFn := mockMutateRowsFn(records, actions...)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: "TestMutateRows_Retry_TransientErrors",
		Request:  dummyMutateRowsRequest(numOfRows),
	}

	// 3. Conduct the test
	res := runMutateRowsTest(t, mockFn, &req, nil)

	// 4a. Check that the overall operation succeeded
	assert.Empty(t, res.GetStatus().GetCode())

	// 4b. Check the number of requests in the records
	assert.Equal(t, numOfRPCs, len(records))

	// 4c. Check the to-be-mutated rows in the requests
	origReq := <-records
	firstRetry := <-records
	secondRetry := <-records

	assert.True(t, proto.Equal(req.Request, origReq.req))
	assert.True(t, proto.Equal(
		dummyMutateRowsRequestCore(
			[]string{"row-1", "row-2"}, []string{"value_1", "value_2"}),
		firstRetry.req))
	assert.True(t, proto.Equal(
		dummyMutateRowsRequestCore(
			[]string{"row-1"}, []string{"value_1"}),
		secondRetry.req))
}

func TestMutateRows_Retry_ExponentialBackoff(t *testing.T) {
	// 0. Common variables
	const numOfRows int = 1
	const numOfRPCs int = 4

	// 1. Instantiate the mock function
	records := make(chan *mutateRowsReqRecord, numOfRPCs + 1)
	actions := []mutateRowsAction{
		mutateRowsAction{ // There is one row to mutate, which has error.
			entries:   buildSimpleEntries(nil, []int{0}, codes.Unavailable),
			isLastRes: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has error.
			entries:   buildSimpleEntries(nil, []int{0}, codes.Unavailable),
			isLastRes: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has error.
			entries:   buildSimpleEntries(nil, []int{0}, codes.Unavailable),
			isLastRes: true,
		},
		mutateRowsAction{ // There is one row to mutate, which has no error.
			entries: buildSimpleEntries([]int{0}, nil, 0),
		},
	}
	mockFn := mockMutateRowsFn(records, actions...)

	// 2. Build the request to test proxy
	req := testproxypb.MutateRowsRequest{
		ClientId: "TestMutateRows_Retry_ExponentialBackoff",
		Request:  dummyMutateRowsRequest(numOfRows),
	}

	// 3. Conduct the test
	runMutateRowsTest(t, mockFn, &req, nil)

	// 4a. Check the number of requests in the records
	assert.Equal(t, numOfRPCs, len(records))

	// 4b. Log the time intervals between consecutive requests
	origReq := <-records
	firstRetry := <-records
	secondRetry := <-records
	thirdRetry := <-records

	firstDelay := int(firstRetry.ts.UnixMilli() - origReq.ts.UnixMilli())
	secondDelay := int(secondRetry.ts.UnixMilli() - firstRetry.ts.UnixMilli())
	thirdDelay := int(thirdRetry.ts.UnixMilli() - secondRetry.ts.UnixMilli())

	// As different clients may have different implementations, we just log the delays here.
	// Example: For the intitial retry delay, C++ client uses 100ms but Java client uses 10ms.
	// Java client allows the second delay to be smaller than the first delay but C++ client doesn't.
	t.Logf("The three retry delays are: %dms, %dms, %dms", firstDelay, secondDelay, thirdDelay)
}
