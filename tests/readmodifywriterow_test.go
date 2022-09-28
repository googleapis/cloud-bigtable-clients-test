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
	"encoding/binary"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/protobuf/testing/protocmp"
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

// TestReadModifyWriteRow_NoRetry_MultiValues tests that client can increment & append multiple values.
func TestReadModifyWriteRow_NoRetry_MultiValues(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"str1", "str2"}
	rowKey := []byte("row-01")
	clientReq := dummyReadModifyWriteRowRequest("table", rowKey, increments, appends)

	// 1. Instantiate the mockserver function
	recorder := make(chan *readModifyWriteRowReqRecord, 1)
	action := &readModifyWriteRowAction{row: dummyResultRow(rowKey, increments, appends)}
	mockFn := mockReadModifyWriteRowFnSimple(recorder, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: t.Name(),
		Request:  clientReq,
	}

	// 3. Perform the operation via test proxy
	res := doReadModifyWriteRowOp(t, mockFn, &req, nil)

	// 4. Check that the dummy request is sent and the dummy row is returned
	checkResultOkStatus(t, res)
	loggedReq := <- recorder
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

	// 1. Instantiate the mockserver function
	recorder := make(chan *readModifyWriteRowReqRecord, requestRecorderCapacity)
	actions := make([]*readModifyWriteRowAction, concurrency)
	for i := 0; i < concurrency; i++ {
		actions[i] = &readModifyWriteRowAction{
			row: dummyResultRow([]byte(rowKeys[i]), increments, appends),
			delayStr: "2s"}
	}
	mockFn := mockReadModifyWriteRowFnSimple(recorder, actions...)

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
	results := doReadModifyWriteRowOps(t, mockFn, reqs, nil)

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
