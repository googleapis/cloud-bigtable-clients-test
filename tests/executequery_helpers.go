package tests

import (
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

func md(columns ...*btpb.ColumnMetadata) *btpb.ExecuteQueryResponse {
	return &btpb.ExecuteQueryResponse{
		Response: &btpb.ExecuteQueryResponse_Metadata{
			Metadata: &btpb.ResultSetMetadata{
				Schema: &btpb.ResultSetMetadata_ProtoSchema{
					ProtoSchema: &btpb.ProtoSchema{
						Columns: columns,
					},
				},
			},
		},
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
		// If it's the final chunk, set the token
		if i == chunks-1 {
			tokenProto = []byte(token)
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
					ResumeToken: tokenProto,
				},
			},
		}
		res[i] = partialResult
	}

	return res
}
