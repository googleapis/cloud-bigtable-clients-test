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

//go:build !emulator
// +build !emulator

package tests

import (
	"testing"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// TestTypedReadRows_MultipleRows_Success verifies multiple rows sent across three batches with
// chained cumulative CRC32C checksums.
func TestTypedReadRows_MultipleRows_Success(t *testing.T) {
	row1 := makeTypedRow([]byte("row1"),
		makeTypedFamily("fam1",
			makeTypedColumn([]byte("col1"), makeTypedCell([]byte("val1"))),
		),
	)
	row2 := makeTypedRow([]byte("row2"),
		makeTypedFamily("fam1",
			makeTypedColumn([]byte("col2"), makeTypedCell([]byte("val2"))),
		),
	)
	row3 := makeTypedRow([]byte("row3"),
		makeTypedFamily("fam2",
			makeTypedColumn([]byte("col3"), makeTypedCell([]byte("val3"))),
		),
	)
	row4 := makeDefaultTypedRow("row4", "val4")

	server := initMockServer(t)
	server.TypedReadRowsFn = mockTypedReadRowsFn(nil,
		typedFlushAction("token1", []*btpb.TypedRow{row1, row2}),
		typedFlushAction("token2", []*btpb.TypedRow{row3}, []*btpb.TypedRow{row1, row2}),
		typedFlushAction("token3", []*btpb.TypedRow{row4}, []*btpb.TypedRow{row1, row2}, row3),
	)

	res := doTypedReadRowsOp(t, server, makeProxyTypedReadRowsRequest(t, "test-table"), nil)

	checkResultOkStatus(t, res)
	assertTypedRowsEqual(t, []*btpb.TypedRow{row1, row2, row3, row4}, res.Rows)
}
