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
	"log"
	"time"

	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
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
	valueSize       int32
	status          RowStatus
}

// readRowsAction denotes an error or a response in the response stream for a ReadRows request.
// Although Client provides both ReadRow() and ReadRows(), server only uses ReadRows() to handle
// them both. Therefore, we don't define readRowAction.
// Usage:
//  1. readRowsAction{chunks: data}
//     Effect: server will return the chunks, and there may be more to come.
//  2. readRowsAction{chunks: data, delayStr: delay}
//     Effect: server will return the chunks after delay, and there may be more to come.
//  3. readRowsAction{rpcError: error}
//     Effect: server will return an error. chunks that are specified in the same action will be ignored.
//  4. readRowsAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. chunks that are specified in the same action will be ignored.
//  5. readRowsAction{rpcError: error, routingCookie: cookie
//     Effect: server will return an error with the routing cookie. Retry attempt header should have this cookie.
//  6. To have a response stream with/without errors, a sequence of actions should be constructed.
type readRowsAction struct {
	chunks        []chunkData
	rpcError      codes.Code
	delayStr      string  // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
	routingCookie string
}
func (a *readRowsAction) Validate() {
	for _, chunk := range a.chunks {
		if len(chunk.rowKey) == 0 && chunk.status == Drop {
			log.Fatal("Drop status cannot be applied to an empty-rowkey chunk")
		}
	}
}

// sampleRowKeysAction denotes an error or a response in the response stream for a SampleRowKeys request.
// Usage:
//  1. sampleRowKeysAction{rowKey: key, offsetBytes: bytes}
//     Effect: server will return a sampled row key with offset, and there may be more to come.
//  2. sampleRowKeysAction{rowKey: key, offsetBytes: bytes, delayStr: delay}
//     Effect: server will return a sampled row key with offset after delay, and there may be more to come.
//  3. sampleRowKeysAction{rowKey: key, offsetBytes: bytes, endOfStream: true}
//     Effect: server will return the last sampled row key with offset.
//  4. sampleRowKeysAction{rowKey: key, offsetBytes: bytes, endOfStream: true, delayStr: delay}
//     Effect: server will return the last sampled row key with offset after delay.
//  5. sampleRowKeysAction{rpcError: error}
//     Effect: server will return an error. rowKey and offsetBytes that are specified in the same action will be ignored.
//  6. sampleRowKeysAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. rowKey and offsetBytes that are specified in the same action will be ignored.
//  7. To have a response stream with/without errors, a sequence of actions should be constructed.
//  8. "endOfStream = true" is used only for concurrency testing.
type sampleRowKeysAction struct {
	rowKey      []byte
	offsetBytes int64
	endOfStream bool   // If true, server will conclude the serving stream for the request.
	rpcError    codes.Code
	delayStr    string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}

// mutateRowAction tells the mock server how to respond to a MutateRow request.
// There is no response stream, so server will conclude serving after performing an action.
// Usage:
//  1. mutateRowAction{}
//     Effect: server will mutate the row successfully (no return value needed).
//  2. mutateRowAction{delayStr: delay}
//     Effect: server will mutate the row successfully after delay (no return value needed).
//  3. mutateRowAction{rpcError: error}
//     Effect: server will return an error.
//  4. mutateRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay.
//  5. To have a successful mutation after transient errors, a sequence of actions should be constructed.
type mutateRowAction struct {
	rpcError codes.Code
	delayStr string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}
func (a *mutateRowAction) Validate() {}

// entryData represents a simplified version of []btpb.MutateRowsResponse_Entry to facilitate
// test writing. The entryData contain a subset of results for bulk mutation request. Note that
// the indices correspond to the requested row mutations, so the same row mutation may have
// different indices in the retries.
type entryData struct {
	mutatedRows []int                // Indices of rows mutated
	failedRows  map[codes.Code][]int // Indices of failed rows, along with their error codes
}

// mutateRowsAction denotes a response in the response stream for a MutateRows request.
// Usage:
//  1. mutateRowsAction{data: data}
//     Effect: server will return the current batch of mutation results, and there may be more to come.
//  2. mutateRowsAction{data: data, delayStr: delay}
//     Effect: server will return the current batch of mutation results after delay, and there may be more to come.
//  3. mutateRowsAction{data: data, endOfStream: true}
//     Effect: server will return the current (also the last) batch of mutation results.
//  4. mutateRowsAction{data: data, endOfStream: true, delayStr: delay}
//     Effect: server will return the current (also the last) batch of mutation results after delay.
//  5. mutateRowsAction{rpcError: error}
//     Effect: server will return an error, without performing any row mutation.
//  6. mutateRowsAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay, without performing any row mutation.
//  7. To have a response stream with/without rpc errors, a sequence of actions should be constructed.
//  8. "endOfStream = true" is not needed if there are no subsequent actions for a request.
type mutateRowsAction struct {
	data        entryData
	endOfStream bool       // If set, server will conclude the serving for the request.
	rpcError    codes.Code // The error is not specific to a particular row (we use entryData instead).
	delayStr    string     // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}
func (a *mutateRowsAction) Validate() {}

// checkAndMutateRowAction tells the mock server how to respond to a CheckAndMutateRow request.
// There is no response stream, so server will conclude serving after performing an action.
// Usage:
//  1. checkAndMutateRowAction{predicateMatched: result}
//     Effect: server will return the predicate match result.
//  2. checkAndMutateRowAction{predicateMatched: result, delayStr: delay}
//     Effect: server will return the predicate match result after delay.
//  3. checkAndMutateRowAction{rpcError: error}
//     Effect: server will return an error. Any specified predicateMatched in the same action will be ignored.
//  4. checkAndMutateRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. Any specified predicateMatched in the same action will be ignored.
//  5. To have a successful action after transient errors, a sequence of actions should be constructed.
type checkAndMutateRowAction struct {
	predicateMatched bool
	rpcError         codes.Code
	delayStr         string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}
func (a *checkAndMutateRowAction) Validate() {}

// readModifyWriteRowAction tells the mock server how to respond to a ReadModifyWriteRow request.
// There is no response stream, so server will conclude serving after performing an action.
// Usage:
//  1. readModifyWriteRowAction{row: row}
//     Effect: server will return the updated row.
//  2. readModifyWriteRowAction{row: row, delayStr: delay}
//     Effect: server will return the updated row after delay.
//  3. readModifyWriteRowAction{rpcError: error}
//     Effect: server will return an error. Any specified row in the same action will be ignored.
//  4. readModifyWriteRowAction{rpcError: error, delayStr: delay}
//     Effect: server will return an error after delay. Any specified row in the same action will be ignored.
//  5. To have a successful action after transient errors, a sequence of actions should be constructed.
type readModifyWriteRowAction struct {
	row      *btpb.Row
	rpcError codes.Code
	delayStr string // "" means zero delay; follow https://pkg.go.dev/time#ParseDuration otherwise
}
func (a *readModifyWriteRowAction) Validate() {}

// readRowsReqRecord allows the mock server to record the received ReadRowsRequest with timestamp.
type readRowsReqRecord struct {
	req *btpb.ReadRowsRequest
	ts  time.Time
}
func (r *readRowsReqRecord) GetTs() time.Time {return r.ts}

// sampleRowKeysReqRecord allows the mock server to record the received SampleRowKeysRequest with timestamp.
type sampleRowKeysReqRecord struct {
	req *btpb.SampleRowKeysRequest
	ts  time.Time
}
func (r *sampleRowKeysReqRecord) GetTs() time.Time {return r.ts}

// mutateRowReqRecord allows the mock server to record the received MutateRowRequest with timestamp.
type mutateRowReqRecord struct {
	req *btpb.MutateRowRequest
	ts  time.Time
}
func (r *mutateRowReqRecord) GetTs() time.Time {return r.ts}

// mutateRowsReqRecord allows the mock server to record the received MutateRowsRequest with timestamp.
type mutateRowsReqRecord struct {
	req *btpb.MutateRowsRequest
	ts  time.Time
}
func (r *mutateRowsReqRecord) GetTs() time.Time {return r.ts}

// checkAndMutateRowReqRecord allows the mock server to record the received CheckAndMutateRowRequest with timestamp.
type checkAndMutateRowReqRecord struct {
	req *btpb.CheckAndMutateRowRequest
	ts  time.Time
}
func (r *checkAndMutateRowReqRecord) GetTs() time.Time {return r.ts}

// readModifyWriteRowReqRecord allows the mock server to record the received ReadModifyWriteRowRequest with timestamp.
type readModifyWriteRowReqRecord struct {
	req *btpb.ReadModifyWriteRowRequest
	ts  time.Time
}
func (r *readModifyWriteRowReqRecord) GetTs() time.Time {return r.ts}

// anyRequest is an interface type that works for the request types of test proxy.
type anyRequest interface {
	*testproxypb.ReadRowRequest | *testproxypb.ReadRowsRequest | *testproxypb.MutateRowRequest |
	*testproxypb.MutateRowsRequest | *testproxypb.SampleRowKeysRequest |
	*testproxypb.CheckAndMutateRowRequest | *testproxypb.ReadModifyWriteRowRequest
	GetClientId() string
}

// anyResult is an interface type that works for the result types of test proxy.
type anyResult interface {
	*testproxypb.RowResult | *testproxypb.RowsResult | *testproxypb.MutateRowResult |
	*testproxypb.MutateRowsResult | *testproxypb.SampleRowKeysResult |
	*testproxypb.CheckAndMutateRowResult
	GetStatus() *status.Status
}

// anyRecord is an interface type that works for the record types defined above.
type anyRecord interface {
	*readRowsReqRecord | *sampleRowKeysReqRecord | *mutateRowReqRecord | *mutateRowsReqRecord |
	*checkAndMutateRowReqRecord | *readModifyWriteRowReqRecord
	GetTs() time.Time
}

// anyAction is an interface type that works for the action types of mock server, except for sampleRowKeysAction.
type anyAction interface {
	*readRowsAction | *mutateRowAction | *mutateRowsAction |
	*checkAndMutateRowAction | *readModifyWriteRowAction
	Validate()
}

// clientOpts contains the custom settings of app profile id and timeout, which are used
// when creating a client object in the test proxy.
type clientOpts struct {
	profile string
	timeout *durationpb.Duration
}

