package tests

import (
	"hash/crc32"
	"log"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32cChecksum(contents []byte) *uint32 {
	checksum := crc32.Checksum(contents, crc32cTable)
	return &checksum
}

func md(columns ...*btpb.ColumnMetadata) *btpb.ResultSetMetadata {
	return &btpb.ResultSetMetadata{
		Schema: &btpb.ResultSetMetadata_ProtoSchema{
			ProtoSchema: &btpb.ProtoSchema{
				Columns: columns,
			},
		},
	}
}

func prepareResponse(preparedQuery []byte, md *btpb.ResultSetMetadata) *btpb.PrepareQueryResponse {
	return prepareResponseWithExpiry(preparedQuery, md, time.Now().Add(1*time.Hour))
}

func prepareResponseWithExpiry(preparedQuery []byte, md *btpb.ResultSetMetadata, validUntil time.Time) *btpb.PrepareQueryResponse {
	ts := tspb.New(validUntil)
	return &btpb.PrepareQueryResponse{
		PreparedQuery: preparedQuery,
		Metadata:      md,
		ValidUntil:    ts,
	}
}

func column(name string, t *btpb.Type) *btpb.ColumnMetadata {
	return &btpb.ColumnMetadata{
		Name: name,
		Type: t,
	}
}

func strType() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_StringType{
			StringType: &btpb.Type_String{},
		},
	}
}

func strValue(v string) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_StringValue{
			StringValue: v,
		},
	}
}

func testProxyMd(columns ...*btpb.ColumnMetadata) *testproxypb.ResultSetMetadata {
	return &testproxypb.ResultSetMetadata{
		Columns: columns,
	}
}

func testProxyRow(values ...*btpb.Value) *testproxypb.SqlRow {
	return &testproxypb.SqlRow{
		Values: values,
	}
}

func partialResultSet(token string, values ...*btpb.Value) *btpb.ExecuteQueryResponse {
	return chunkedPartialResultSet(1, token, values...)[0]
}

func chunkedPartialResultSet(chunks int, token string, values ...*btpb.Value) []*btpb.ExecuteQueryResponse {
	protoRows := &btpb.ProtoRows{
		Values: values,
	}
	rowBytes, err := proto.Marshal(protoRows)
	if err != nil {
		log.Fatalln("Failed to encode protoRows:", err)
	}

	res := make([]*btpb.ExecuteQueryResponse, chunks)
	chunkSize := int(len(rowBytes) / chunks)
	for i := 0; i < chunks; i++ {
		var tokenProto []byte = nil
		var checksum *uint32 = nil
		// If it's the final chunk, set the token and checksum
		if i == chunks-1 {
			tokenProto = []byte(token)
			checksum = crc32cChecksum(rowBytes)
		}
		dataChunk := rowBytes[i*chunkSize : (i+1)*chunkSize]
		partialResult := &btpb.ExecuteQueryResponse{
			Response: &btpb.ExecuteQueryResponse_Results{
				Results: &btpb.PartialResultSet{
					PartialRows: &btpb.PartialResultSet_ProtoRowsBatch{
						ProtoRowsBatch: &btpb.ProtoRowsBatch{
							BatchData: dataChunk,
						},
					},
					ResumeToken:   tokenProto,
					BatchChecksum: checksum,
				},
			},
		}
		res[i] = partialResult
	}

	return res
}
