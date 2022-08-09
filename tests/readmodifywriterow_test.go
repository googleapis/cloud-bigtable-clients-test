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
		RowKey:    []byte("row-01"),
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
		Key: []byte("row-01"),
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

// TestReadModifyWriteRow_Basic_MultiValues tests that client can increment & append multiple values.
func TestReadModifyWriteRow_Basic_MultiValues(t *testing.T) {
	// 0. Common variables
	increments := []int64{10, 2}
	appends := []string{"append"}
	rowKey := []byte("row-01")
	clientReq := dummyReadModifyWriteRowRequest("table", rowKey, increments, appends)

	// 1. Instantiate the mock function
	records := make(chan *readModifyWriteRowReqRecord, 1)
	action := readModifyWriteRowAction{row: dummyResultRow(rowKey, increments, appends)}
	mockFn := mockReadModifyWriteRowFn(records, action)

	// 2. Build the request to test proxy
	req := testproxypb.ReadModifyWriteRowRequest{
		ClientId: "TestReadModifyWriteRow_Basic_MultiValues",
		Request:  clientReq,
	}

	// 3. Conduct the test
	res := runReadModifyWriteRowTest(t, mockFn, &req, nil)

	// 4. Check that dummy request is sent, and dummyRow is returned.
	assert.Empty(t, res.GetStatus().GetCode())
	loggedReq := <- records
	if diff := cmp.Diff(clientReq, loggedReq.req, protocmp.Transform()); diff != "" {
		t.Errorf("diff found (-want +got):\n%s", diff)
	}
	assert.Equal(t, "row-01", string(res.Row.Key))
	assert.Equal(t, 10 + 2, int(binary.BigEndian.Uint64(res.Row.Families[0].Columns[0].Cells[0].Value)))
	assert.Equal(t, "append", string(res.Row.Families[0].Columns[1].Cells[0].Value))
}

