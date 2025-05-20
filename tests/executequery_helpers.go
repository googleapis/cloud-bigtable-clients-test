package tests

import (
	"crypto/rand"
	"hash/crc32"
	"log"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"github.com/googleapis/cloud-bigtable-clients-test/testproxypb"
	datepb "google.golang.org/genproto/googleapis/type/date"
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

func bytesType() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_BytesType{
			BytesType: &btpb.Type_Bytes{},
		},
	}
}

func int64Type() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_Int64Type{
			Int64Type: &btpb.Type_Int64{},
		},
	}
}

func boolType() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_BoolType{
			BoolType: &btpb.Type_Bool{},
		},
	}
}

func float32Type() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_Float32Type{
			Float32Type: &btpb.Type_Float32{},
		},
	}
}

func float64Type() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_Float64Type{
			Float64Type: &btpb.Type_Float64{},
		},
	}
}

func timestampType() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_TimestampType{
			TimestampType: &btpb.Type_Timestamp{},
		},
	}
}

func dateType() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_DateType{
			DateType: &btpb.Type_Date{},
		},
	}
}

func structType(fields ...*btpb.Type_Struct_Field) *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_StructType{
			StructType: &btpb.Type_Struct{
				Fields: fields,
			},
		},
	}
}

func structField(name string, t *btpb.Type) *btpb.Type_Struct_Field {
	return &btpb.Type_Struct_Field{
		FieldName: name,
		Type:      t,
	}
}

func namelessStructField(t *btpb.Type) *btpb.Type_Struct_Field {
	return &btpb.Type_Struct_Field{
		Type: t,
	}
}

func arrayType(element *btpb.Type) *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_ArrayType{
			ArrayType: &btpb.Type_Array{
				ElementType: element,
			},
		},
	}
}

func mapType(key *btpb.Type, value *btpb.Type) *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_MapType{
			MapType: &btpb.Type_Map{
				KeyType:   key,
				ValueType: value,
			},
		},
	}
}

func strVal(v string) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_StringValue{
			StringValue: v,
		},
	}
}

func strValWithType(v string) *btpb.Value {
	res := strVal(v)
	res.Type = strType()
	return res
}

func bytesVal(v []byte) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_BytesValue{
			BytesValue: v,
		},
	}
}

func bytesValWithType(v []byte) *btpb.Value {
	res := bytesVal(v)
	res.Type = bytesType()
	return res
}

func intVal(v int64) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_IntValue{
			IntValue: v,
		},
	}
}

func intValWithType(v int64) *btpb.Value {
	res := intVal(v)
	res.Type = int64Type()
	return res
}

func boolVal(v bool) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_BoolValue{
			BoolValue: v,
		},
	}
}

func boolValWithType(v bool) *btpb.Value {
	res := boolVal(v)
	res.Type = boolType()
	return res
}

func floatVal(v float64) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_FloatValue{
			FloatValue: v,
		},
	}
}

func float32ValWithType(v float64) *btpb.Value {
	res := floatVal(v)
	res.Type = float32Type()
	return res
}

func float64ValWithType(v float64) *btpb.Value {
	res := floatVal(v)
	res.Type = float64Type()
	return res
}

func timestampVal(seconds int64, nanos int32) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_TimestampValue{
			TimestampValue: &tspb.Timestamp{Seconds: seconds, Nanos: nanos},
		},
	}
}

func timestampValWithType(seconds int64, nanos int32) *btpb.Value {
	res := timestampVal(seconds, nanos)
	res.Type = timestampType()
	return res
}

func dateVal(year int32, month int32, day int32) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_DateValue{
			DateValue: &datepb.Date{
				Year:  year,
				Month: month,
				Day:   day,
			},
		},
	}
}

func dateValWithType(year int32, month int32, day int32) *btpb.Value {
	res := dateVal(year, month, day)
	res.Type = dateType()
	return res
}

func structVal(fields ...*btpb.Value) *btpb.Value {
	return arrayVal(fields...)
}

func arrayVal(elements ...*btpb.Value) *btpb.Value {
	return &btpb.Value{
		Kind: &btpb.Value_ArrayValue{
			ArrayValue: &btpb.ArrayValue{
				Values: elements,
			},
		},
	}
}

func arrayValWithType(elementType *btpb.Type, elements ...*btpb.Value) *btpb.Value {
	res := arrayVal(elements...)
	res.Type = arrayType(elementType)
	return res
}

func mapVal(entries ...[]*btpb.Value) *btpb.Value {
	var kvArray []*btpb.Value
	for _, elem := range entries {
		kvArray = append(kvArray, arrayVal(elem...))
	}
	return arrayVal(kvArray...)
}

func mapEntry(k *btpb.Value, v *btpb.Value) []*btpb.Value {
	return []*btpb.Value{k, v}
}

func nullVal() *btpb.Value {
	return &btpb.Value{}
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
		if i == chunks-1 && token != "" {
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

func generateBytes(n int64) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatalln("Failed to generate bytes: ", err)
	}

	return b
}
