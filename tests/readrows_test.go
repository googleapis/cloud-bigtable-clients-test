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

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
)

// dummyChunkData returns a chunkData object with hardcoded family name and qualifier.
func dummyChunkData(rowKey string, value string, status RowStatus) chunkData {
	return chunkData{
		rowKey: []byte(rowKey), familyName: "f", qualifier: "col", value: value, status: status}
}

// TestReadRows_Generic_Headers tests that ReadRows request has client and resource info in the
// header.
func TestReadRows_Generic_Headers(t *testing.T) {
	// 0. Common variables
	tableName := buildTableName("table")

	// 1. Instantiate the mock function
	// Don't call mockReadRowsFn() as the behavior is to record metadata of the request.
	mdRecords := make(chan metadata.MD, 1)
	mockFn := func(req *btpb.ReadRowsRequest, srv btpb.Bigtable_ReadRowsServer) error {
		md, _ := metadata.FromIncomingContext(srv.Context())
		mdRecords <- md
		return nil
	}

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.ReadRowsRequest{TableName: tableName},
	}

	// 3. Perform the operation via test proxy
	doReadRowsOp(t, mockFn, &req, nil)

	// 4. Check the request headers in the metadata
	md := <-mdRecords
	assert.NotEmpty(t, md["x-goog-api-client"])
	assert.Contains(t, md["x-goog-request-params"][0], tableName)
}

// TestReadRow_NoRetry_PointReadDeadline tests that client will set deadline for point read.
func TestReadRow_NoRetry_PointReadDeadline(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *readRowsReqRecord, 1)
	oneResponse := readRowsAction{
		chunks:   []chunkData{dummyChunkData("row-01", "v1", Commit)},
		delayStr: "5s",
	}
	mockFn := mockReadRowsFn(records, oneResponse)

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
	res := doReadRowOp(t, mockFn, &req, &timeout)

	// 4a. Check the runtime
	curTs := time.Now()
	origReq := <-records
	runTimeSecs := int(curTs.Unix() - origReq.ts.Unix())
	assert.GreaterOrEqual(t, runTimeSecs, 2)
	assert.Less(t, runTimeSecs, 5)

	// 4b. Check the DeadlineExceeded error
	assert.Equal(t, int32(codes.DeadlineExceeded), res.GetStatus().GetCode())
}

// TestReadRows_NoRetry_OutOfOrderError tests that client will fail on receiving out of order row keys.
func TestReadRows_NoRetry_OutOfOrderError(t *testing.T) {
	// 1. Instantiate the mock function
	oneResponse := readRowsAction{
		chunks: []chunkData{
			dummyChunkData("row-01", "v1", Commit),
			// The following two rows are in bad order
			dummyChunkData("row-07", "v7", Commit),
			dummyChunkData("row-03", "v3", Commit),
		},
	}
	mockFn := mockReadRowsFn(nil, oneResponse)

	// 2. Build the request to test proxyk
	req := testproxypb.ReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.ReadRowsRequest{TableName: buildTableName("table")},
	}

	// 3. Perform the operation via test proxy
	res := doReadRowsOp(t, mockFn, &req, nil)

	// 4. Check the response (C++ and Java clients have different error messages)
	assert.Contains(t, res.GetStatus().GetMessage(), "increasing")
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}

// TestReadRows_NoRetry_ErrorAfterLastRow tests that when receiving a transient error after receiving
// the last row, the read will still finish successfully.
func TestReadRows_NoRetry_ErrorAfterLastRow(t *testing.T) {
	// 1. Instantiate the mock function
	stream := []readRowsAction{
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("row-01", "v1", Commit)}},
		readRowsAction{rpcError: codes.DeadlineExceeded}, // Error after returning the requested row
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("row-05", "v5", Commit)}},
	}
	mockFn := mockReadRowsFn(nil, stream...)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.ReadRowsRequest{
			TableName: buildTableName("table"),
			RowsLimit: 1,
		},
	}

	// 3. Perform the operation via test proxy
	res := doReadRowsOp(t, mockFn, &req, nil)

	// 4. Verify that the read succeeds
	assert.Empty(t, res.GetStatus().GetCode())
	assert.Equal(t, 1, len(res.GetRow()))
	assert.Equal(t, "row-01", string(res.Row[0].Key))
}

// TestReadRows_Retry_PausedScan tests that client will transparently resume the scan when a stream
// is paused.
func TestReadRows_Retry_PausedScan(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *readRowsReqRecord, 2)
	stream := []readRowsAction{
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("row-01", "v1", Commit)}},
		readRowsAction{rpcError: codes.Aborted}, // close the stream by aborting it
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("row-05", "v5", Commit)}},
	}
	mockFn := mockReadRowsFn(records, stream...)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.ReadRowsRequest{TableName: buildTableName("table")},
	}

	// 3. Perform the operation via test proxy
	res := doReadRowsOp(t, mockFn, &req, nil)

	// 4a. Verify that two rows were read successfully
	assert.Empty(t, res.GetStatus().GetCode())
	assert.Equal(t, 2, len(res.GetRow()))
	assert.Equal(t, "row-01", string(res.Row[0].Key))
	assert.Equal(t, "row-05", string(res.Row[1].Key))

	// 4b. Verify that client sent the retry request properly
	loggedReq := <-records
	loggedRetry := <-records
	assert.Empty(t, loggedReq.req.GetRows().GetRowRanges())
	assert.True(t, cmp.Equal(loggedRetry.req.GetRows().GetRowRanges()[0].StartKey, &btpb.RowRange_StartKeyOpen{StartKeyOpen: []byte("row-01")}))
}

// TestReadRows_Retry_LastScannedRow tests that client will resume from last scan row key.
func TestReadRows_Retry_LastScannedRow(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *readRowsReqRecord, 2)
	stream := []readRowsAction{
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("abar", "v_a", Commit)}},
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("qfoo", "v_q", Drop)}}, // Chunkless response due to Drop
		readRowsAction{rpcError: codes.DeadlineExceeded}, // Server-side DeadlineExceeded should be retry-able.
		readRowsAction{
			chunks: []chunkData{
				dummyChunkData("zbar", "v_z", Commit)}},
	}
	mockFn := mockReadRowsFn(records, stream...)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowsRequest{
		ClientId: t.Name(),
		Request:  &btpb.ReadRowsRequest{TableName: buildTableName("table")},
	}

	// 3. Perform the operation via test proxy
	res := doReadRowsOp(t, mockFn, &req, nil)

	// 4a. Verify that rows aabar and zzbar were read successfully (qqfoo doesn't match the filter)
	assert.Empty(t, res.GetStatus().GetCode())
	assert.Equal(t, 2, len(res.GetRow()))
	assert.Equal(t, "abar", string(res.Row[0].Key))
	assert.Equal(t, "zbar", string(res.Row[1].Key))

	// 4b. Verify that client sent the retry request properly
	loggedReq := <-records
	loggedRetry := <-records
	assert.Empty(t, loggedReq.req.GetRows().GetRowRanges())
	assert.True(t, cmp.Equal(loggedRetry.req.GetRows().GetRowRanges()[0].StartKey, &btpb.RowRange_StartKeyOpen{StartKeyOpen: []byte("qfoo")}))
}

// TestReadRow_NoRetry_CommitInSeparateChunk tests that client can have one chunk
// with no status and subsequent chunk with a commit status.
func TestReadRow_NoRetry_CommitInSeparateChunk(t *testing.T) {
	// 1. Instantiate the mock function
	records := make(chan *readRowsReqRecord, 1)
	oneResponse := readRowsAction{
		chunks: []chunkData{
			chunkData{rowKey: []byte("row-01"), familyName: "A", qualifier: "Qw1", timestampMicros: 99, value: "dmFsdWUtVkFM", status: None},
			chunkData{familyName: "B", qualifier: "Qw2", timestampMicros: 102, value: "dmFsdWUtVkFJ", status: Commit},
		},
	}

	mockFn := mockReadRowsFn(records, oneResponse)

	// 2. Build the request to test proxy
	req := testproxypb.ReadRowRequest{
		ClientId:  t.Name(),
		TableName: buildTableName("table"),
		RowKey:    "row-01",
	}

	// 3. Perform the operation via test proxy
	res := doReadRowOp(t, mockFn, &req, nil)

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
