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

// +build emulator

// Tests in this file uses an in-memory emulator for Cloud Bigtable, so as to
// build comprehensive or long-running workload. For more
// information, please check https://cloud.google.com/bigtable/docs/emulator.
package tests

import (
	"context"
	"os"
	"regexp"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
	"google.golang.org/grpc"
)

// createTableInEmulator creates a table with the given table id and family id.
// As the test proxy doesn't support admin APIs, use go client here.
func createTableInEmulator(addr string, tableID string, familyID string) error {
	// Connect to the emulator
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()
	adminClient, err := bigtable.NewAdminClient(
		ctx, projectID, instanceID, option.WithGRPCConn(conn))
	defer adminClient.Close()
	if err != nil {
		return err
	}

	// Create table
	err = adminClient.CreateTable(ctx, tableID)
	if err != nil {
		return err
	}

	// Create family
	err = adminClient.CreateColumnFamily(ctx, tableID, familyID)
	if err != nil {
		return err
	}
	return nil
}

// TestEmulator_EnvVar tests that client can connect to emulator via environment
// variable BIGTABLE_EMULATOR_HOST. For this reason, the test is different from the
// other mockserver-based tests:
//   1. Before bringing up the test proxy, set BIGTABLE_EMULATOR_HOST as
//      localhost:<your selected server port>
//   2. Before launching this test, you should also set BIGTABLE_EMULATOR_HOST as above.
//
// As the use of BIGTABLE_EMULATOR_HOST may introduce failure (e.g., it's not set properly),
// the test will exit gracefully on the related error.
func TestEmulator_EnvVar(t *testing.T) {
	// 0. Common variables
	const tableID string = "table"
	const rowKey string = "row-01"
	const family string = "f"
	const column string = "col"
	const value string = "emulator_value"
	clientID := t.Name()

	// 1. Bring up the emulator using address in BIGTABLE_EMULATOR_HOST
	emulatorAddr := os.Getenv("BIGTABLE_EMULATOR_HOST")
	r := regexp.MustCompile("localhost:[1-9][0-9]*")
	if !r.MatchString(emulatorAddr) {
		t.Logf("BIGTABLE_EMULATOR_HOST is not set properly: %s, skip the test", emulatorAddr)
		return
	}
	srv, err := bttest.NewServer(emulatorAddr)
	if err != nil {
		t.Logf("Fail to start emulator: %v, skip the test", err)
		return
	}
	defer srv.Close()

	// 2. Create a table in the emulator
	err = createTableInEmulator(srv.Addr, tableID, family)
	if err != nil {
		t.Fatalf("Fail to create table: %v", err)
	}

	// 3. Create the client that connects to the emulator
	createCbtClient(t, clientID, "emulator", nil)
	defer removeCbtClient(t, clientID)
	defer closeCbtClient(t, clientID)

	// 4. Write some data
	mutateReq := &testproxypb.MutateRowRequest{
		ClientId: clientID,
		Request:  &btpb.MutateRowRequest{
			TableName: buildTableName(tableID),
			RowKey: []byte(rowKey),
			Mutations: []*btpb.Mutation{
				&btpb.Mutation{
					Mutation: &btpb.Mutation_SetCell_{
						SetCell: &btpb.Mutation_SetCell{
							FamilyName:      family,
							ColumnQualifier: []byte(column),
							Value:           []byte(value),
						},
					},
				},
			},
		},
	}
	mutateRes := doMutateRowOpsCore(t, clientID, []*testproxypb.MutateRowRequest{mutateReq}, nil)
	checkResultOkStatus(t, mutateRes...)

	// 5. Read and validate the data
	readReq := &testproxypb.ReadRowRequest{
		ClientId:  clientID,
		TableName: buildTableName(tableID),
		RowKey:    rowKey,
	}
	readRes := doReadRowOpsCore(t, clientID, []*testproxypb.ReadRowRequest{readReq}, nil)
	checkResultOkStatus(t, readRes...)
	assert.Equal(t, rowKey, string(readRes[0].Row.GetKey()))
	assert.Equal(t, value, string(readRes[0].Row.Families[0].Columns[0].Cells[0].GetValue()))
}

