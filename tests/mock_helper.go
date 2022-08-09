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
	"context"
	"log"
	"os"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	gs "google.golang.org/grpc/status"
)

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

// mockReadRowsFn returns a mock implementation of server-side ReadRows().
// The behavior is customized by `actions` which will be validated to avoid misuse of "Drop" status.
// Non-nil `records` will be used to log the requests (including retries) received by the server in
// time order, up to its allocated capacity.
func mockReadRowsFn(records chan<- *readRowsReqRecord, actions ...readRowsAction) func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *readRowsAction, len(actions))
	for i := range actions {
		for _, chunk := range actions[i].chunks {
			if len(chunk.rowKey) == 0 && chunk.status == Drop {
				log.Fatal("Drop status cannot be applied to empty-rowkey chunk")
			}
		}
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(req *btpb.ReadRowsRequest, srv btpb.Bigtable_ReadRowsServer) error {
		if records != nil {
			reqRecord := readRowsReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("ReadRows request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				return gs.Error(action.rpcError, "ReadRows failed")
			}

			res := &btpb.ReadRowsResponse{}
			var lastRowKey []byte
			for _, chunk := range action.chunks{
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
// The behavior is customized by `actions`. Non-nil `records` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
func mockSampleRowKeysFn(records chan<- *sampleRowKeysReqRecord, actions ...sampleRowKeysAction) func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *sampleRowKeysAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(req *btpb.SampleRowKeysRequest, srv btpb.Bigtable_SampleRowKeysServer) error {
		if records != nil {
			reqRecord := sampleRowKeysReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("SampleRowKeys request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
				return gs.Error(action.rpcError, "SampleRowKeys failed")
			}

			res := &btpb.SampleRowKeysResponse{
				RowKey:      action.rowKey,
				OffsetBytes: action.offsetBytes,
			}
			srv.Send(res)
		}
		return nil
	}
}

// mockMutateRowFn returns a mock implementation of server-side MutateRow().
// The behavior is customized by `actions`. Non-nil `records` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
func mockMutateRowFn(records chan<- *mutateRowReqRecord, actions ...mutateRowAction) func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *mutateRowAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
		if records != nil {
			reqRecord := mutateRowReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("MutateRow request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		action := <-actionQueue
		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "MutateRow failed")
		}

		return &btpb.MutateRowResponse{}, nil
	}
}

// mockMutateRowsFn returns a mock implementation of server-side MutateRows().
// The behavior is customized by `actions`. Non-nil `records` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
func mockMutateRowsFn(records chan<- *mutateRowsReqRecord, actions ...mutateRowsAction) func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *mutateRowsAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(req *btpb.MutateRowsRequest, srv btpb.Bigtable_MutateRowsServer) error {
		if records != nil {
			reqRecord := mutateRowsReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("MutateRows request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		for {
			action, more := <-actionQueue
			if !more {
				break
			}
			sleepFor(action.delayStr)

			if action.rpcError != codes.OK {
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

// mockCheckAndMutateRowFn returns a mock implementation of server-side CheckAndMutateRow().
// The behavior is customized by `actions`. Non-nil `records` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
func mockCheckAndMutateRowFn(records chan<- *checkAndMutateRowReqRecord, actions ...checkAndMutateRowAction) func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *checkAndMutateRowAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(ctx context.Context, req *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
		if records != nil {
			reqRecord := checkAndMutateRowReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("CheckAndMutateRow request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		action := <-actionQueue
		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "CheckAndMutateRow failed")
		}

		return &btpb.CheckAndMutateRowResponse{PredicateMatched: action.predicateMatched}, nil
	}
}

// mockReadModifyWriteRowFn returns a mock implementation of server-side ReadModifyWriteRow().
// The behavior is customized by `actions`. Non-nil `records` will be used to log the requests
// (including retries) received by the server in time order, up to its allocated capacity.
func mockReadModifyWriteRowFn(records chan<- *readModifyWriteRowReqRecord, actions ...readModifyWriteRowAction) func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	// Enqueue the actions, and server will consume the queue via FIFO.
	actionQueue := make(chan *readModifyWriteRowAction, len(actions))
	for i := range actions {
		actionQueue <- &actions[i]
	}
	close(actionQueue)

	return func(ctx context.Context, req *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
		if records != nil {
			reqRecord := readModifyWriteRowReqRecord{
				req: req,
				ts:  time.Now(),
			}
			select {
			case records <- &reqRecord:
			default:
				serverLogger.Printf("ReadModifyWriteRow request is not logged as channel records is full: capacity %d", cap(records))
			}
		}

		action := <-actionQueue
		sleepFor(action.delayStr)

		if action.rpcError != codes.OK {
			return nil, gs.Error(action.rpcError, "ReadModifyWriteRow failed")
		}

		return &btpb.ReadModifyWriteRowResponse{Row: action.row}, nil
	}
}

