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
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	gs "google.golang.org/grpc/status"
	drpb "google.golang.org/protobuf/types/known/durationpb"
	wrappers "google.golang.org/protobuf/types/known/wrapperspb"
)

var rowKeyPrefixRegex = regexp.MustCompile("^op[0-9]+-")
var serverLogger *log.Logger = log.New(os.Stderr, "[Servr log] ", log.Flags())

func sleepFor(duration string) {
	if duration == "" {
		return
	}

	d, err := time.ParseDuration(duration)
	if err == nil {
		serverLogger.Printf("There is %s sleep on the server side\n", duration)
		time.Sleep(d)
	} else {
		serverLogger.Printf("Failed to parse the duration string %s. Skip the sleep\n", duration)
	}
}

// expandDim inserts a length 1 dimension to the input array, and returns the resultant 2-D array.
// Input: [a1_for_req1, a2_for_req2, ..., aN_for_reqN] -- There is one action per request.
// Output: [[a1_for_req1], [a2_for_req2], ..., [aN_for_reqN]]
//
//	-- There is one action sequence per request, and each sequence only contains one element.
//
//	-- There is one action sequence per request, and each sequence only contains one element.
func expandDim[A anyAction](actions []A) [][]A {
	actionSequences := make([][]A, len(actions))
	for i, action := range actions {
		actionSequences[i] = []A{action}
	}
	return actionSequences
}

// buildActionMap builds a map from rowkey prefix "optX-" to the relevant action sequence.
// In the process, validation is performed on each action.
func buildActionMap[A anyAction](opIDToActionQueue map[string]chan A, actionSequences [][]A) {
	for opID, actionSequence := range actionSequences {
		// Convert the action sequence to a queue for server to consume.
		actionQueue := make(chan A, len(actionSequence))
		for _, action := range actionSequence {
			action.Validate()
			actionQueue <- action
		}
		close(actionQueue)
		opIDToActionQueue[fmt.Sprintf("op%d-", opID)] = actionQueue
	}
}

// saveReqRecord saves the `record` by pushing it to `recorder`.
func saveReqRecord[R anyRecord](recorder chan<- R, record R) {
	if recorder != nil {
		select {
		case recorder <- record:
		default:
			serverLogger.Printf("Request is not saved as the recorder runs out of capacity: %d", cap(recorder))
		}
	}
}

// retrieveActions returns server actions based on the prefix of the first rowKey in the request.
func retrieveActions[A anyAction](opIDToActionQueue map[string]chan A, rowKey []byte) (chan A, error) {
	// If it's for non-concurrency testing
	if len(opIDToActionQueue) == 1 {
		return opIDToActionQueue["op0-"], nil
	}

	// Now we know it's for concurrency testing
	if len(rowKey) == 0 {
		return nil, gs.Error(codes.InvalidArgument, "rowkey must be set for concurrency testing")
	}

	prefix := rowKeyPrefixRegex.Find(rowKey)
	if prefix == nil {
		return nil, gs.Error(codes.InvalidArgument, "rowkey must have prefix opX-")
	}

	var ok bool
	var actionQueue chan A
	if actionQueue, ok = opIDToActionQueue[string(prefix)]; !ok {
		return nil, gs.Error(codes.InvalidArgument, "Didn't find actions with key "+string(prefix))
	}

	return actionQueue, nil
}

// mockReadRowsFnSimple is a simple wrapper of mockReadRowsFn. It's useful when server only performs
// one action per request, as users don't need to assemble an array of actions per request.
func mockReadRowsFnSimple(recorder chan<- *readRowsReqRecord, actions ...*readRowsAction) func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error {
	actionSequences := expandDim(actions)
	return mockReadRowsFn(recorder, actionSequences...)
}

func mockReadRowsFn(recorder chan<- *readRowsReqRecord, actionSequences ...[]*readRowsAction) func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error {
	return mockReadRowsFnWithMetadata(recorder, nil, actionSequences...)
}

// mockReadRowsFn returns a mock implementation of server-side ReadRows(). The behavior is
// customized by `actionSequences`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its capacity.
// For concurrency testing, each request MUST have prefix "opX-" in the row key(s), indicating
// that the X-th (zero based) actionSequence will be used to serve the request.
func mockReadRowsFnWithMetadata(recorder chan<- *readRowsReqRecord, mdRecorder chan metadata.MD, actionSequences ...[]*readRowsAction) func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error {
	// Build the map so that server can retrieve the proper action queue by key "opX-".
	opIDToActionQueue := make(map[string]chan *readRowsAction)
	buildActionMap(opIDToActionQueue, actionSequences)

	return func(req *btpb.ReadRowsRequest, srv btpb.Bigtable_ReadRowsServer) error {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the metadata
		if mdRecorder != nil {
			md, _ := metadata.FromIncomingContext(srv.Context())
			mdRecorder <- md
		}

		// Record the request
		reqRecord := &readRowsReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Select the actions to perform
		var rowKey []byte
		if req.GetRows() != nil && len(req.GetRows().GetRowKeys()) > 0 {
			rowKey = req.GetRows().GetRowKeys()[0]
		} else if len(opIDToActionQueue) > 1 {
			return gs.Error(codes.InvalidArgument, "The ReadRows request must contain rowkeys for concurrency testing")
		}

		actionQueue, err := retrieveActions(opIDToActionQueue, rowKey)
		if err != nil {
			return err
		}

		// Perform the actions
		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				if action.routingCookie != "" {
					// add routing cookie to metadata
					trailer := metadata.Pairs("x-goog-cbt-cookie-test", action.routingCookie)
					srv.SetTrailer(trailer)
				}
				// TODO check for feature flag
				if action.retryInfo != "" {
					st := gs.New(action.rpcError, "ReadRows failed")
					delay, _ := time.ParseDuration(action.retryInfo)
					retryInfo := &errdetails.RetryInfo{
						RetryDelay: drpb.New(delay),
					}
					st, err = st.WithDetails(retryInfo)
					return st.Err()
				}
				return gs.Error(action.rpcError, "ReadRows failed")
			}

			res := &btpb.ReadRowsResponse{}
			var lastRowKey []byte
			for _, chunk := range action.chunks {
				if chunk.status == Drop {
					lastRowKey = chunk.rowKey
					continue
				}

				lastRowKey = []byte{}
				cellChunk := &btpb.ReadRowsResponse_CellChunk{
					RowKey:          chunk.rowKey,
					FamilyName:      &wrappers.StringValue{Value: chunk.familyName},
					Qualifier:       &wrappers.BytesValue{Value: []byte(chunk.qualifier)},
					TimestampMicros: chunk.timestampMicros,
					Value:           []byte(chunk.value),
					ValueSize:       chunk.valueSize,
				}
				if chunk.status == Commit {
					cellChunk.RowStatus = &btpb.ReadRowsResponse_CellChunk_CommitRow{CommitRow: true}
				} else if chunk.status == Reset {
					cellChunk.RowStatus = &btpb.ReadRowsResponse_CellChunk_ResetRow{ResetRow: true}
				}
				res.Chunks = append(res.Chunks, cellChunk)
			}

			if len(res.Chunks) > 0 {
				srv.Send(res)
			}

			// res can set either Chunks or LastScannedRowKey, but not both.
			// So two responses may be sent for a readRowsAction.
			if len(lastRowKey) > 0 {
				res.Reset()
				res.LastScannedRowKey = lastRowKey
				srv.Send(res)
			}
		}
		return nil
	}
}

// mockSampleRowKeysFn returns a mock implementation of server-side SampleRowKeys().
// The behavior is customized by `actions`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
// Unlike the other methods, the concurrency testing is quite restricted as we cannot differentiate
// the requests by row keys (only table name is specified in the request).
func mockSampleRowKeysFn(recorder chan<- *sampleRowKeysReqRecord, actions []sampleRowKeysAction) func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error {
	return mockSampleRowKeysFnWithMetadata(recorder, nil, actions)
}

func mockSampleRowKeysFnWithMetadata(recorder chan<- *sampleRowKeysReqRecord, mdRecorder chan metadata.MD, actions []sampleRowKeysAction) func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *sampleRowKeysAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(req *btpb.SampleRowKeysRequest, srv btpb.Bigtable_SampleRowKeysServer) error {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the metadata
		if mdRecorder != nil {
			md, _ := metadata.FromIncomingContext(srv.Context())
			mdRecorder <- md
		}

		// Record the request
		reqRecord := &sampleRowKeysReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				if action.routingCookie != "" {
					// add routing cookie to metadata
					trailer := metadata.Pairs("x-goog-cbt-cookie-test", action.routingCookie)
					srv.SetTrailer(trailer)
				}
				if action.retryInfo != "" {
					st := gs.New(action.rpcError, "SampleRowKeys failed")
					delay, _ := time.ParseDuration(action.retryInfo)
					retryInfo := &errdetails.RetryInfo{
						RetryDelay: drpb.New(delay),
					}
					st, _ = st.WithDetails(retryInfo)
					return st.Err()
				}
				return gs.Error(action.rpcError, "SampleRowKeys failed")
			}

			res := &btpb.SampleRowKeysResponse{
				RowKey:      action.rowKey,
				OffsetBytes: action.offsetBytes,
			}
			srv.Send(res)

			if action.endOfStream {
				return nil
			}
		}
		return nil
	}
}

// mockMutateRowFnSimple is a simple wrapper of mockMutateRowFn. It's useful when server only
// performs one action per request, as users don't need to assemble an array of actions per request.
func mockMutateRowFnSimple(recorder chan<- *mutateRowReqRecord, actions ...*mutateRowAction) func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	actionSequences := expandDim(actions)
	return mockMutateRowFn(recorder, actionSequences...)
}

// mockMutateRowFn returns a mock implementation of server-side MutateRow(). The behavior is
// customized by `actionSequences`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
// For concurrency testing, each request MUST have prefix "opX-" in the row key, indicating
// that the X-th (zero based) actionSequence will be used to serve the request.
func mockMutateRowFn(recorder chan<- *mutateRowReqRecord, actionSequences ...[]*mutateRowAction) func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	// Build the map so that server can retrieve the proper action queue by key "opX-".
	opIDToActionQueue := make(map[string]chan *mutateRowAction)
	buildActionMap(opIDToActionQueue, actionSequences)

	return func(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the request
		reqRecord := &mutateRowReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Select the actions to perform
		rowKey := req.GetRowKey()
		actionQueue, err := retrieveActions(opIDToActionQueue, rowKey)
		if err != nil {
			return nil, err
		}

		// Perform the actions
		action := <-actionQueue
		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "MutateRow failed")
		}

		return &btpb.MutateRowResponse{}, nil
	}
}

// mockMutateRowsFnSimple is a simple wrapper of mockMutateRowsFn. It's useful when server only
// performs one action per request, as users don't need to assemble an array of actions per request.
func mockMutateRowsFnSimple(recorder chan<- *mutateRowsReqRecord, actions ...*mutateRowsAction) func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error {
	actionSequences := expandDim(actions)
	return mockMutateRowsFn(recorder, actionSequences...)
}

// mockMutateRowsFn returns a mock implementation of server-side MutateRows(). The behavior is
// customized by `actionSequences`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
// For concurrency testing, each request MUST have prefix "opX-" in the row keys, indicating
// that the X-th (zero based) actionSequence will be used to serve the request.
func mockMutateRowsFn(recorder chan<- *mutateRowsReqRecord, actionSequences ...[]*mutateRowsAction) func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error {
	return mockMutateRowsFnWithMetadata(recorder, nil, actionSequences...)
}

func mockMutateRowsFnWithMetadata(recorder chan<- *mutateRowsReqRecord, mdRecorder chan metadata.MD, actionSequences ...[]*mutateRowsAction) func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error {
	// Build the map so that server can retrieve the proper action queue by key "opX-".
	opIDToActionQueue := make(map[string]chan *mutateRowsAction)
	buildActionMap(opIDToActionQueue, actionSequences)

	return func(req *btpb.MutateRowsRequest, srv btpb.Bigtable_MutateRowsServer) error {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the metadata
		if mdRecorder != nil {
			md, _ := metadata.FromIncomingContext(srv.Context())
			mdRecorder <- md
		}

		// Record the request
		reqRecord := &mutateRowsReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Select the actions to perform
		var rowKey []byte
		if len(req.GetEntries()) > 0 {
			rowKey = req.GetEntries()[0].GetRowKey()
		} else if len(opIDToActionQueue) > 1 {
			return gs.Error(codes.InvalidArgument, "The MutateRows request must contain entries for concurrency testing")
		}

		actionQueue, err := retrieveActions(opIDToActionQueue, rowKey)
		if err != nil {
			return err
		}

		// Perform the actions
		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				if action.routingCookie != "" {
					// add routing cookie to metadata
					trailer := metadata.Pairs("x-goog-cbt-cookie-test", action.routingCookie)
					srv.SetTrailer(trailer)
				}
				if action.retryInfo != "" {
					st := gs.New(action.rpcError, "MutateRows failed")
					delay, _ := time.ParseDuration(action.retryInfo)
					retryInfo := &errdetails.RetryInfo{
						RetryDelay: drpb.New(delay),
					}
					st, err = st.WithDetails(retryInfo)
					return st.Err()
				}
				return gs.Error(action.rpcError, "MutateRows failed")
			}

			res := &btpb.MutateRowsResponse{}
			// Fill in entries for rows mutated successfully
			for _, idx := range action.data.mutatedRows {
				res.Entries = append(res.Entries, &btpb.MutateRowsResponse_Entry{
					Index:  int64(idx),
					Status: &status.Status{},
				})
			}
			// Fill in entries for rows with errors
			for errorCode, idxs := range action.data.failedRows {
				for _, idx := range idxs {
					res.Entries = append(res.Entries, &btpb.MutateRowsResponse_Entry{
						Index:  int64(idx),
						Status: &status.Status{Code: int32(errorCode)},
					})
				}
			}
			srv.Send(res)

			if action.endOfStream {
				return nil
			}
		}
		return nil
	}
}

// mockCheckAndMutateRowFnSimple is a simple wrapper of mockCheckAndMutateRowFn. It's useful when
// server only performs one action per request, as users don't need to assemble an array of actions
// per request.
func mockCheckAndMutateRowFnSimple(recorder chan<- *checkAndMutateRowReqRecord, actions ...*checkAndMutateRowAction) func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	actionSequences := expandDim(actions)
	return mockCheckAndMutateRowFn(recorder, actionSequences...)
}

// mockCheckAndMutateRowFn returns a mock implementation of server-side CheckAndMutateRow().
// The behavior is customized by `actionSequences`. Non-nil `recorder` will be used to log the
// requests (including retries) received by the server in time order, up to its allocated capacity.
// For concurrency testing, each request MUST have prefix "opX-" in the row key, indicating
// that the X-th (zero based) actionSequence will be used to serve the request.
func mockCheckAndMutateRowFn(recorder chan<- *checkAndMutateRowReqRecord, actionSequences ...[]*checkAndMutateRowAction) func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	// Build the map so that server can retrieve the proper action queue by key "opX-".
	opIDToActionQueue := make(map[string]chan *checkAndMutateRowAction)
	buildActionMap(opIDToActionQueue, actionSequences)

	return func(ctx context.Context, req *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the request
		reqRecord := &checkAndMutateRowReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Select the actions to perform
		rowKey := req.GetRowKey()
		actionQueue, err := retrieveActions(opIDToActionQueue, rowKey)
		if err != nil {
			return nil, err
		}

		// Perform the action
		action := <-actionQueue
		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "CheckAndMutateRow failed")
		}

		return &btpb.CheckAndMutateRowResponse{PredicateMatched: action.predicateMatched}, nil
	}
}

// mockReadModifyWriteRowFnSimple is a simple wrapper of mockReadModifyWriteRowFn. It's useful when
// server only performs one action per request, as users don't need to assemble an array of actions
// per request.
func mockReadModifyWriteRowFnSimple(recorder chan<- *readModifyWriteRowReqRecord, actions ...*readModifyWriteRowAction) func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	actionSequences := expandDim(actions)
	return mockReadModifyWriteRowFn(recorder, actionSequences...)
}

// mockReadModifyWriteRowFn returns a mock implementation of server-side ReadModifyWriteRow().
// The behavior is customized by `actionSequences`. Non-nil `recorder` will be used to log the
// requests (including retries) received by the server in time order, up to its allocated capacity.
// For concurrency testing, each request MUST have prefix "opX-" in the row key, indicating
// that the X-th (zero based) actionSequence will be used to serve the request.
func mockReadModifyWriteRowFn(recorder chan<- *readModifyWriteRowReqRecord, actionSequences ...[]*readModifyWriteRowAction) func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	opIDToActionQueue := make(map[string]chan *readModifyWriteRowAction)
	buildActionMap(opIDToActionQueue, actionSequences)

	return func(ctx context.Context, req *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the request
		reqRecord := &readModifyWriteRowReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Select the actions to perform
		rowKey := req.GetRowKey()
		actionQueue, err := retrieveActions(opIDToActionQueue, rowKey)
		if err != nil {
			return nil, err
		}

		// Perform the action
		action := <-actionQueue
		sleepFor(action.delayStr)
		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "ReadModifyWriteRow failed")
		}

		return &btpb.ReadModifyWriteRowResponse{Row: action.row}, nil
	}
}

// mockExecuteQueryFnSimple is a simple wrapper of mockExecuteQueryFn. It's useful when server only performs
// one action per request, as users don't need to assemble an array of actions per request.
func mockExecuteQueryFn(recorder chan<- *executeQueryReqRecord, actionSequence ...*executeQueryAction) func(*btpb.ExecuteQueryRequest, btpb.Bigtable_ExecuteQueryServer) error {
	return mockExecuteQueryFnWithMetadataSimple(recorder, nil, actionSequence...)
}

// helper for using mockExecuteQueryFnWithMetadata with a single action sequence
func mockExecuteQueryFnWithMetadataSimple(recorder chan<- *executeQueryReqRecord, mdRecorder chan metadata.MD, actionSequence ...*executeQueryAction) func(*btpb.ExecuteQueryRequest, btpb.Bigtable_ExecuteQueryServer) error {
	actionSequences := make(map[string][]*executeQueryAction, 1)
	actionSequences["onlySequence"] = actionSequence
	return mockExecuteQueryFnWithMetadata(recorder, mdRecorder, actionSequences)
}

// mockExecuteQueryFn returns a mock implementation of server-side ExecuteQuery(). The behavior is
// customized by `actionSequences`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its capacity.
// If only one actionSequence is specified then it is used for all requests. If multiple are
// specified this expects the Request PreparedQuery to be the map key of the sequence it should use.
func mockExecuteQueryFnWithMetadata(recorder chan<- *executeQueryReqRecord, mdRecorder chan metadata.MD, actionSequences map[string][]*executeQueryAction) func(*btpb.ExecuteQueryRequest, btpb.Bigtable_ExecuteQueryServer) error {
	return func(req *btpb.ExecuteQueryRequest, srv btpb.Bigtable_ExecuteQueryServer) error {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// if there is only one action sequence use it. Otherwise
		// we assume the prepared query is a utf-8 string of the an int of the sequence index
		var selectedActionSequence []*executeQueryAction
		if len(actionSequences) == 1 {
			selectedActionSequence = actionSequences["onlySequence"]
		} else {
			selectedActionSequence = actionSequences[string(req.PreparedQuery)]
		}

		actionQueue := make(chan *executeQueryAction, len(selectedActionSequence))
		for _, action := range selectedActionSequence {
			action.Validate()
			actionQueue <- action
		}

		// Record the metadata
		if mdRecorder != nil {
			md, _ := metadata.FromIncomingContext(srv.Context())
			mdRecorder <- md
		}

		// Record the request
		reqRecord := &executeQueryReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Perform the actions
		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				if action.routingCookie != "" {
					// add routing cookie to metadata
					trailer := metadata.Pairs("x-goog-cbt-cookie-test", action.routingCookie)
					srv.SetTrailer(trailer)
				}
				if action.retryInfo != "" {
					st := gs.New(action.rpcError, "ExecuteQuery failed")
					delay, _ := time.ParseDuration(action.retryInfo)
					retryInfo := &errdetails.RetryInfo{
						RetryDelay: drpb.New(delay),
					}
					st, _ = st.WithDetails(retryInfo)
					return st.Err()
				}
				return gs.Error(action.rpcError, "ExecuteQuery failed")
			}

			if action.response != nil {
				srv.Send(action.response)
			}

			if action.endOfStream {
				return nil
			}
		}
		return nil
	}
}

// mockPrepareQueryFn is a simple wrapper of mockPrepareQueryFnWithMetadata. It doesn't record
// any metadata
func mockPrepareQueryFn(recorder chan<- *prepareQueryReqRecord, actions ...*prepareQueryAction) func(context.Context, *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
	return mockPrepareQueryFnWithMetadata(recorder, nil, actions...)
}

// mockPrepareQueryFnWithMatchingQuery returns a mock implementation of server-side PrepareQuery(). The behavior is
// customized by `queryMap`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its capacity.
// The key of query map is used to find the appropriate prepareQueryAction based on the request Query.
// Requests with the same query will reuse the same prepareQueryAction multiple times.
func mockPrepareQueryFnWithMatchingQuery(recorder chan<- *prepareQueryReqRecord, queryMap map[string]*prepareQueryAction) func(context.Context, *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
	return func(ctx context.Context, req *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
		prepareAction := queryMap[req.Query]
		return mockPrepareQueryFn(recorder, prepareAction)(ctx, req)
	}
}

// mockPrepareQueryFnWithMetadata returns a mock implementation of server-side PrepareQuery(). The behavior is
// customized by `actions`. Non-nil `recorder` will be used to log the requests
// (including retries) received by the server in time order, up to its capacity.
// The prepareQueryActions in actions will be used in order for each request.
func mockPrepareQueryFnWithMetadata(recorder chan<- *prepareQueryReqRecord, mdRecorder chan metadata.MD, actions ...*prepareQueryAction) func(context.Context, *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
	// Convert the action sequence to a queue for server to consume.
	actionQueue := make(chan *prepareQueryAction, len(actions))
	for _, action := range actions {
		action.Validate()
		actionQueue <- action
	}
	close(actionQueue)

	return func(ctx context.Context, req *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
		if *printClientReq {
			serverLogger.Printf("Request from client: %+v", req)
		}

		// Record the metadata
		if mdRecorder != nil {
			md, _ := metadata.FromIncomingContext(ctx)
			mdRecorder <- md
		}

		// Record the request
		reqRecord := &prepareQueryReqRecord{
			req: req,
			ts:  time.Now(),
		}
		saveReqRecord(recorder, reqRecord)

		// Perform the action
		action := <-actionQueue

		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "PrepareQuery failed")
		}

		return action.response, nil
	}
}
