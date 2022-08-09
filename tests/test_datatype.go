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
//
// This file defines data types that allow the test cases to customize the
// behavior of mock server:
// "<operation>Action" types tell the server to either perform the operation
// or return an error. You can build a sequence of actions to make the server
// return streaming responses and (or) respond to ordered retry attempts,
// where the server will perform each action only once.
// "<operation>ReqRecord" types let the server log the request from client
// with timestamp.

package tests

import (
	"time"

	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc/codes"
)

// RowStatus is a Enum type to hold value for read row's status ranging from None
// to Drop below.
type RowStatus int

const (
	None RowStatus = iota // Indicates the row hasn't been returned completely
	Commit // Indicates that all the chunks of a row have been returned
	Reset  // Indicates that the row will be re-read
	Drop   // Indicates a row is filtered out from ReadRows response
)

// chunkData represents a simplified version of btpb.ReadRowsResponse_CellChunk to facilitate
// test writing; it also simulates row filtering by adding "Drop" status, in addition to
// "Commit" and "Reset".
//
// For "Commit" or "Reset" status, rowKey can be empty to imply the continuation of a row whose
// key has been sent back in the previous chunks.
//
// For "Drop" status, rowKey cannot be empty (test library will validate it) to avoid confusion
// of dropping a chunk vs. dropping a row:
//     ...
//     chunkData{rowKey: "row", status: None, ...}
//     chunkData{rowKey: "", status: None, ...}
//     chunkData{rowKey: "", status: Drop, ...}
//     ...
//
// Guidance:
//     - To drop a row, a single chunk is enough:
//       ...
//       chunkData{rowKey: "row", status: Drop, ...}
//       ...
//     - To drop a chunk, removing it from the response stream is enough:
//       ...
//       chunkData{rowKey: "row", status: None, ...}
//       chunkData{rowKey: "", status: Commit, ...}
//       ...
type chunkData struct {
	rowKey          []byte
	familyName      string
	qualifier       string
	timestampMicros int64
	value           string
	status          RowStatus
}

// readRowsAction denotes an error or a datapoint in the response stream for a ReadRows request.
// Although Client provides both ReadRow() and ReadRows(), server only provides ReadRows() to
// handle them both. So we don't have readRowAction defined. Usage:
//    1. readRowsAction{chunks: data}
//       Effect: server will return ReadRowsResponse based on the given chunks.
//    2. readRowsAction{chunks: data, delayStr: delay}
//       Effect: server will return ReadRowsResponse based on the given chunks after delay.
//    3. readRowsAction{rpcError: error}
//       Effect: server will return an error. Any chunks that are specified will be ignored.
//    4. readRowsAction{rpcError: error, delayStr: delay}
//       Effect: server will return an error after delay. Any chunks that are specified will be ignored.
//    5. To combine chunks with errors, a sequence of actions should be constructed.
type readRowsAction struct {
	chunks   []chunkData
	rpcError codes.Code
	delayStr string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// sampleRowKeysAction denotes an error or a datapoint in the response stream for a SampleRowKeys request.
// Usage:
//  1. sampleRowKeysAction{rowKey: key, offsetBytes: bytes}
//     Effect: server will return a SampleRowKeysResponse using the given row key and offset bytes.
//  2. sampleRowKeysAction{rowKey: key, offsetBytes: bytes, delayStr: delay}
//     Effect: server will return a SampleRowKeysResponse using the given row key and offset bytes after delay.
//  3. sampleRowKeysAction{rpcError: error}
//     Effect: server will return an error. Any rowKey/offsetBytes that are specified will be ignored.
//  4. sampleRowKeysAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. Any rowKey/offsetBytes that are specified will be ignored.
//  5. To combine rowKey/offsetBytes with errors, a sequence of actions should be constructed.
type sampleRowKeysAction struct {
	rowKey      []byte
	offsetBytes int64
	rpcError    codes.Code
	delayStr    string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// mutateRowAction tells the mock server how to respond to a MutateRow request.
// There is no response stream. Usage:
//  1. mutateRowAction{}
//     Effect: server will mutate the row successfully (no return value needed).
//  2. mutateRowAction{delayStr: delay}
//     Effect: server will mutate the row successfully after delay (no return value needed).
//  3. mutateRowAction{rpcError: error}
//     Effect: server will return an error.
//  4. mutateRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay.
type mutateRowAction struct {
	rpcError codes.Code
	delayStr string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// entryData represents a simplified version of []btpb.MutateRowsResponse_Entry to facilitate
// test writing. The entryData may contain the results of a subset of row mutations. Note that
// the indices correspond to the requested row mutations, so the same row mutation may be given
// different indices in the retries.
type entryData struct {
	mutatedRows []int                // Indices of rows mutated
	failedRows  map[codes.Code][]int // Indices of failed rows, along with their error codes
}

// mutateRowsAction denotes a datapoint in the response stream for a MutateRows request.
// Usage:
//  1. mutateRowsAction{data: data}
//     Effect: server will return the current mutation results, and more will come.
//  2. mutateRowsAction{data: data, delayStr: delay}
//     Effect: server will return the current mutation results after delay, and more will come.
//  3. mutateRowsAction{data: data, endOfStream: true}
//     Effect: server will return the current (also the last) batch of mutation results.
//  4. mutateRowsAction{data: data, endOfStream: true, delayStr: delay}
//     Effect: server will return the current (also the last) batch of mutation results after delay.
//  5. mutateRowsAction{rpcError: error}
//     Effect: server will return an error, without performing any row mutation.
//  6. mutateRowsAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay, without performing any row mutation.
//  7. To combine data with rpc errors, a sequence of actions should be constructed.
//  8. "endOfStream = true" is not needed if there are no subsequent actions.
type mutateRowsAction struct {
	data        entryData
	endOfStream bool       // If set, server will conclude the serving stream for the request.
	rpcError    codes.Code // The error is not specific to a particular row (unlike the other methods).
	delayStr    string     // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// checkAndMutateRowAction tells the mock server how to respond to a CheckAndMutateRow request.
// There is no response stream. Usage:
//  1. checkAndMutateRowAction{predicateMatched: result}
//     Effect: server will return the predicate match result.
//  2. checkAndMutateRowAction{predicateMatched: row, delayStr: delay}
//     Effect: server will return the predicate match result after delay.
//  3. checkAndMutateRowAction{rpcError: error}
//     Effect: server will return an error. Any specified predicateMatched will be ignored.
//  4. checkAndMutateRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. Any specified predicateMatched will be ignored.
//  5. To combine predicateMatched with errors, a sequence of actions should be constructed.
type checkAndMutateRowAction struct {
	predicateMatched bool
	rpcError         codes.Code
	delayStr         string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// readModifyWriteRowAction tells the mock server how to respond to a ReadModifyWriteRow request.
// There is no response stream. Usage:
//  1. readModifyWriteRowAction{row: row}
//     Effect: server will return the updated row.
//  2. readModifyWriteRowAction{row: row, delayStr: delay}
//     Effect: server will return the updated row after delay.
//  3. readModifyWriteRowAction{rpcError: error}
//     Effect: server will return an error. Any specified row will be ignored.
//  4. readModifyWriteRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. Any specified row will be ignored.
//  5. To combine row with errors, a sequence of actions should be constructed.
type readModifyWriteRowAction struct {
	row      *btpb.Row
	rpcError codes.Code
	delayStr string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// readRowsReqRecord allows the mock server to record the received ReadRowsRequest with timestamp.
type readRowsReqRecord struct {
	req *btpb.ReadRowsRequest
	ts  time.Time
}

// sampleRowKeysReqRecord allows the mock server to record the received SampleRowKeysRequest with timestamp.
type sampleRowKeysReqRecord struct {
	req *btpb.SampleRowKeysRequest
	ts  time.Time
}

// mutateRowReqRecord allows the mock server to record the received MutateRowRequest with timestamp.
type mutateRowReqRecord struct {
	req *btpb.MutateRowRequest
	ts  time.Time
}

// mutateRowsReqRecord allows the mock server to record the received MutateRowsRequest with timestamp.
type mutateRowsReqRecord struct {
	req *btpb.MutateRowsRequest
	ts  time.Time
}

// checkAndMutateRowReqRecord allows the mock server to record the received CheckAndMutateRowRequest with timestamp.
type checkAndMutateRowReqRecord struct {
	req *btpb.CheckAndMutateRowRequest
	ts  time.Time
}

// readModifyWriteRowReqRecord allows the mock server to record the received ReadModifyWriteRowRequest with timestamp.
type readModifyWriteRowReqRecord struct {
	req *btpb.ReadModifyWriteRowRequest
	ts  time.Time
}

