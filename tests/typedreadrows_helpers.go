// Copyright 2026 Google LLC
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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// chainedChecksum computes the next running checksum in the chain by computing
// CRC32C of the batch bytes followed by the 4-byte little-endian running checksum.
// Matches Bigtable TypedRowMerger: H_k = CRC32C(batch_bytes_k || LE32(H_{k-1})).
func chainedChecksum(running uint32, batch []byte) uint32 {
	h := crc32.New(crc32cTable)
	h.Write(batch)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], running)
	h.Write(buf[:])
	return h.Sum32()
}

// batchChecksum returns the stream checksum for a single initial batch starting from 0.
// Matches Bigtable TypedRowMerger: H_1 = CRC32C(batch_bytes_1 || LE32(0)).
// Equivalent to multiBatchChecksum(data).
func batchChecksum(data []byte) uint32 {
	return chainedChecksum(0, data)
}

// multiBatchChecksum computes the cumulative running checksum across a sequence of
// batches, starting from 0. Empty batches are skipped to mirror TypedRowMerger's rule
// that empty batches do not advance the running checksum.
func multiBatchChecksum(batches ...[]byte) uint32 {
	var running uint32 = 0
	for _, batch := range batches {
		if len(batch) > 0 {
			running = chainedChecksum(running, batch)
		}
	}
	return running
}

// A note on Value kinds, since TypedCell/TypedColumn/TypedRow all carry the general-purpose
// `Value` message and only a narrow slice of it is actually reachable here:
//
//   - TypedCell.value       is ALWAYS raw_value. Bigtable cells are raw bytes; the server never
//                           emits string_value, int_value, array_value or any other kind, and
//                           never leaves the field unset.
//   - TypedColumn.qualifier is ALWAYS raw_value, for the same reason.
//   - TypedRow.row_key      is raw_value when the stream carries no row_key_schema, and
//                           array_value when it does. The client enforces this and fails the read
//                           on a mismatch, so it is the one place a structured Value legitimately
//                           appears in a response.
//
// Do not add constructors for other cell or qualifier kinds. A conformance fixture that models a
// stream the server cannot produce is worse than no fixture, because it invites client authors to
// implement handling for cases that will never arrive. (Request-side TypedRowSet.row_prefixes is a
// separate matter -- data.proto specifies those as array_value.)

// makeTypedCell creates a TypedCell with bytes value.
func makeTypedCell(val []byte) *btpb.TypedCell {
	return makeTypedCellWithTimestamp(val, 0)
}

// makeTypedCellWithTimestamp creates a TypedCell with a timestamp and bytes value.
func makeTypedCellWithTimestamp(val []byte, timestampMicros int64) *btpb.TypedCell {
	return makeTypedCellWithTimestampAndLabels(val, timestampMicros)
}

// makeTypedCellWithTimestampAndLabels creates a TypedCell with a timestamp, bytes value, and labels.
func makeTypedCellWithTimestampAndLabels(val []byte, timestampMicros int64, labels ...string) *btpb.TypedCell {
	return &btpb.TypedCell{
		Timestamp: timestamppb.New(time.UnixMicro(timestampMicros)),
		Value:     rawVal(val),
		Labels:    labels,
	}
}

// makeTypedColumn creates a TypedColumn with a raw_value bytes qualifier and cells.
func makeTypedColumn(qualifier []byte, cells ...*btpb.TypedCell) *btpb.TypedColumn {
	return &btpb.TypedColumn{
		Qualifier: rawVal(qualifier),
		Cells:     cells,
	}
}

// makeTypedFamily creates a TypedFamily with family name and columns.
func makeTypedFamily(familyName string, cols ...*btpb.TypedColumn) *btpb.TypedFamily {
	return &btpb.TypedFamily{
		FamilyName: familyName,
		Columns:    cols,
	}
}

// makeTypedRow creates a TypedRow with row key and families.
func makeTypedRow(rowKey []byte, families ...*btpb.TypedFamily) *btpb.TypedRow {
	return &btpb.TypedRow{
		RowKey:   rawVal(rowKey),
		Families: families,
	}
}

// serializeTypedRows serializes a slice of TypedRow into protobuf bytes (TypedRows).
// Marshal failure is a defect in the test fixture rather than in the client under test,
// so it terminates the binary instead of returning an error. This mirrors splitIntoChunks
// in executequery_helpers.go and keeps call sites free of error-handling boilerplate.
func serializeTypedRows(rows ...*btpb.TypedRow) []byte {
	typedRows := &btpb.TypedRows{
		Rows: rows,
	}
	data, err := proto.Marshal(typedRows)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode TypedRows: %v", err))
	}
	return data
}

// splitBatchIntoChunks splits raw byte data into chunks of at most chunkSize bytes.
func splitBatchIntoChunks(data []byte, chunkSize int) [][]byte {
	if chunkSize <= 0 || len(data) == 0 {
		return [][]byte{data}
	}
	var chunks [][]byte
	for len(data) > chunkSize {
		chunks = append(chunks, data[:chunkSize])
		data = data[chunkSize:]
	}
	if len(data) > 0 {
		chunks = append(chunks, data)
	}
	return chunks
}

// chunkedTypedResponses builds a series of TypedReadRowsResponse messages by fragmenting data into chunks
// and adding a flush with CRC32C and resume token to the final response.
func chunkedTypedResponses(data []byte, chunkSize int, resumeToken []byte, prevBatches ...[]byte) []*btpb.TypedReadRowsResponse {
	chunks := splitBatchIntoChunks(data, chunkSize)
	var responses []*btpb.TypedReadRowsResponse

	for i, chunk := range chunks {
		isLast := (i == len(chunks)-1)

		resp := &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{
						BatchData: chunk,
					},
				},
			},
		}

		if isLast {
			var flushChecksum *uint32
			if len(data) > 0 {
				allBatches := make([][]byte, 0, len(prevBatches)+1)
				allBatches = append(allBatches, prevBatches...)
				allBatches = append(allBatches, data)
				c := multiBatchChecksum(allBatches...)
				flushChecksum = &c
			}
			resp.Response.Flush = &btpb.PartialRowResponse_Flush{
				Checksum:    flushChecksum,
				ResumeToken: resumeToken,
			}
		}

		responses = append(responses, resp)
	}
	return responses
}

// responsesToActions converts a slice of TypedReadRowsResponse into typedReadRowsAction objects.
func responsesToActions(responses ...*btpb.TypedReadRowsResponse) []*typedReadRowsAction {
	actions := make([]*typedReadRowsAction, len(responses))
	for i, r := range responses {
		actions[i] = &typedReadRowsAction{response: r}
	}
	return actions
}

// assertTypedRowsEqual asserts that two slices of TypedRow are equal.
func assertTypedRowsEqual(t *testing.T, expected, actual []*btpb.TypedRow) {
	assert.Equal(t, len(expected), len(actual), "row count mismatch")
	if diff := cmp.Diff(expected, actual, protocmp.Transform()); diff != "" {
		t.Errorf("TypedRow diff (-want +got):\n%s", diff)
	}
}

func buildAuthorizedViewName(tableID, viewID string) string {
	return fmt.Sprintf("projects/%s/instances/%s/tables/%s/authorizedViews/%s", projectID, instanceID, tableID, viewID)
}

func buildMaterializedViewName(mvID string) string {
	return fmt.Sprintf("projects/%s/instances/%s/materializedViews/%s", projectID, instanceID, mvID)
}

func makeTypedReadRowsRequest(tableName string) *btpb.TypedReadRowsRequest {
	return &btpb.TypedReadRowsRequest{
		Target: &btpb.TypedReadRowsRequest_TableName{
			TableName: tableName,
		},
	}
}

// makeDefaultTypedRow creates a simple TypedRow with default family "cf", column "c", and raw byte value.
func makeDefaultTypedRow(rowKey, val string) *btpb.TypedRow {
	return makeTypedRow([]byte(rowKey),
		makeTypedFamily("cf",
			makeTypedColumn([]byte("c"), makeTypedCell([]byte(val))),
		),
	)
}

// serializeBatchArg normalizes one element of a prevBatches list into raw batch bytes.
// An unsupported type is a test-authoring bug: returning nil would silently drop the
// batch from the cumulative checksum and surface as a bogus client-side mismatch, so
// terminate loudly instead.
func serializeBatchArg(arg any) []byte {
	switch v := arg.(type) {
	case []byte:
		return v
	case *btpb.TypedRow:
		return serializeTypedRows(v)
	case []*btpb.TypedRow:
		return serializeTypedRows(v...)
	default:
		panic(fmt.Sprintf("Unsupported prevBatches element type %T", arg))
	}
}

// makeFlushResponse serializes the provided rows into a TypedRowsBatch, computes the cumulative CRC32C checksum
// across prevBatches and current rows, and returns a TypedReadRowsResponse with Flush.
// prevBatches elements can be *btpb.TypedRow, []*btpb.TypedRow, or raw []byte.
//
// IMPORTANT: each prevBatches element is one previously *flush-committed* batch, not one
// previously-sent response. A batch may be chunked across several responses; the client accumulates
// those chunks and folds exactly one CRC per flush. The cumulative checksum does the same, so the
// grouping of these arguments changes the result:
//
//	makeFlushResponse(tok, rows, row1, row2)                   // two prior batches of one row each
//	makeFlushResponse(tok, rows, []*btpb.TypedRow{row1, row2})  // one prior batch of two rows
//
// These yield different checksums. Group the arguments to match how the mock actually
// sent the earlier batches, or the client will be blamed for a mismatch it didn't cause.
func makeFlushResponse(resumeToken string, rows []*btpb.TypedRow, prevBatches ...any) *btpb.TypedReadRowsResponse {
	data := serializeTypedRows(rows...)
	var allBatches [][]byte
	for _, pb := range prevBatches {
		if b := serializeBatchArg(pb); len(b) > 0 {
			allBatches = append(allBatches, b)
		}
	}
	allBatches = append(allBatches, data)
	var flushChecksum *uint32
	if len(data) > 0 {
		crc := multiBatchChecksum(allBatches...)
		flushChecksum = &crc
	}
	return &btpb.TypedReadRowsResponse{
		Response: &btpb.PartialRowResponse{
			PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
				TypedRowsBatch: &btpb.TypedRowsBatch{
					BatchData: data,
				},
			},
			Flush: &btpb.PartialRowResponse_Flush{
				Checksum:    flushChecksum,
				ResumeToken: []byte(resumeToken),
			},
		},
	}
}

// makeCorruptFlushResponse is like makeFlushResponse, except that the Flush checksum bits are flipped
// to simulate an in-transit corruption / checksum mismatch.
func makeCorruptFlushResponse(resumeToken string, rows []*btpb.TypedRow, prevBatches ...any) *btpb.TypedReadRowsResponse {
	resp := makeFlushResponse(resumeToken, rows, prevBatches...)
	corruptCRC := resp.GetResponse().GetFlush().GetChecksum() ^ 0xFFFFFFFF
	resp.Response.Flush.Checksum = &corruptCRC
	return resp
}

// makeDefaultFlushResponse creates a single-row flush response for makeDefaultTypedRow(rowKey, val),
// optionally computing cumulative checksum across prevBatches.
func makeDefaultFlushResponse(rowKey, val, resumeToken string, prevBatches ...any) *btpb.TypedReadRowsResponse {
	return makeFlushResponse(resumeToken, []*btpb.TypedRow{makeDefaultTypedRow(rowKey, val)}, prevBatches...)
}

// typedFlushAction wraps makeFlushResponse in a typedReadRowsAction.
func typedFlushAction(resumeToken string, rows []*btpb.TypedRow, prevBatches ...any) *typedReadRowsAction {
	return &typedReadRowsAction{response: makeFlushResponse(resumeToken, rows, prevBatches...)}
}

// typedDefaultFlushAction wraps makeDefaultFlushResponse in a typedReadRowsAction.
func typedDefaultFlushAction(rowKey, val, resumeToken string, prevBatches ...any) *typedReadRowsAction {
	return &typedReadRowsAction{response: makeDefaultFlushResponse(rowKey, val, resumeToken, prevBatches...)}
}

// typedHeartbeatAction returns a typedReadRowsAction that emits a sparse heartbeat flush.
func typedHeartbeatAction(resumeToken string) *typedReadRowsAction {
	return &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				Flush: &btpb.PartialRowResponse_Flush{
					ResumeToken: []byte(resumeToken),
				},
			},
		},
	}
}

// typedResetAction returns a typedReadRowsAction that emits an in-stream reset signal.
func typedResetAction() *typedReadRowsAction {
	return &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				Reset_: true,
			},
		},
	}
}

// typedUncommittedAction returns a typedReadRowsAction that sends row data without a flush/resume token.
func typedUncommittedAction(rows ...*btpb.TypedRow) *typedReadRowsAction {
	return &typedReadRowsAction{
		response: &btpb.TypedReadRowsResponse{
			Response: &btpb.PartialRowResponse{
				PartialRows: &btpb.PartialRowResponse_TypedRowsBatch{
					TypedRowsBatch: &btpb.TypedRowsBatch{
						BatchData: serializeTypedRows(rows...),
					},
				},
			},
		},
	}
}

// typedDefaultUncommittedAction returns a typedReadRowsAction that sends a single default row without a flush/resume token.
func typedDefaultUncommittedAction(rowKey, val string) *typedReadRowsAction {
	return typedUncommittedAction(makeDefaultTypedRow(rowKey, val))
}

// rawVal constructs a Value with RawValue set (used for unstructured row keys in requests/responses).
func rawVal(b []byte) *btpb.Value {
	return &btpb.Value{Kind: &btpb.Value_RawValue{RawValue: b}}
}

// makeStructRowKeySchema builds a TableSchema whose RowKeySchema has fields with the given types.
func makeStructRowKeySchema(fieldTypes ...*btpb.Type) *btpb.TableSchema {
	fields := make([]*btpb.Type_Struct_Field, len(fieldTypes))
	for i, ft := range fieldTypes {
		fields[i] = structField(fmt.Sprintf("part%d", i+1), ft)
	}
	return &btpb.TableSchema{
		RowKeySchema: &btpb.Type_Struct{
			Fields: fields,
		},
	}
}

// makeStructuredTypedRow creates a TypedRow whose RowKey is an ArrayValue of the given element Values.
func makeStructuredTypedRow(keyParts []*btpb.Value, families ...*btpb.TypedFamily) *btpb.TypedRow {
	return &btpb.TypedRow{
		RowKey:   arrayVal(keyParts...),
		Families: families,
	}
}

// makeProxyTypedReadRowsRequest builds a testproxypb.TypedReadRowsRequest targeting tableID.
func makeProxyTypedReadRowsRequest(t *testing.T, tableID string) *testproxypb.TypedReadRowsRequest {
	return &testproxypb.TypedReadRowsRequest{
		ClientId: t.Name(),
		Request:  makeTypedReadRowsRequest(buildTableName(tableID)),
	}
}

// assertResumeTokens verifies that the recorder received len(expectedTokens) requests and that
// each request's ResumeToken matches the corresponding entry in expectedTokens (an empty string
// asserts an empty ResumeToken). It returns the recorded requests in order.
func assertResumeTokens(t *testing.T, recorder <-chan *typedReadRowsReqRecord, expectedTokens ...string) []*btpb.TypedReadRowsRequest {
	t.Helper()
	if !assert.Equal(t, len(expectedTokens), len(recorder), "unexpected number of recorded TypedReadRows requests") {
		t.FailNow()
	}
	reqs := make([]*btpb.TypedReadRowsRequest, len(expectedTokens))
	for i, wantTok := range expectedTokens {
		rec := <-recorder
		reqs[i] = rec.req
		if wantTok == "" {
			assert.Empty(t, rec.req.GetResumeToken(), "attempt %d expected empty resume_token", i+1)
		} else {
			assert.Equal(t, []byte(wantTok), rec.req.GetResumeToken(), "attempt %d resume_token mismatch", i+1)
		}
	}
	return reqs
}

// assertTypedReadRowsFailure asserts that a TypedReadRows operation failed with a non-OK status
// and yielded no rows.
func assertTypedReadRowsFailure(t *testing.T, res *testproxypb.TypedRowsResult, msg string) {
	t.Helper()
	assert.NotNil(t, res)
	assert.NotEqual(t, int32(codes.OK), res.GetStatus().GetCode(), msg)
	assert.Empty(t, res.GetRows(), "rows must not be yielded on failure (%s)", msg)
	t.Logf("The full error message is: %s", res.GetStatus().GetMessage())
}
