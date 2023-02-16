import test_proxy_pb2
from bigtable_pb2 import ReadRowsResponse, ReadRowsRequest

from google.protobuf.wrappers_pb2 import StringValue, BytesValue
def simple_reads(client_id, num_rows=100000, payload_size=1, chunks_per_response=100):
    """
    A large number of simple row reads
    should test max throughput of read_rows
    """
    def server_response_fn():
        sent_num = 0
        while sent_num < num_rows:
            batch_size = min(chunks_per_response, num_rows-sent_num)
            chunks = [
                ReadRowsResponse.CellChunk(
                    row_key=(sent_num+i).to_bytes(3, "big"),
                    family_name=StringValue(value="A"),
                    qualifier=BytesValue(value=b"Qw=="), 
                    value=("a"*int(payload_size)).encode(),
                    commit_row=True
                ) for i in range(batch_size)
            ]
            yield ReadRowsResponse(chunks=chunks)
            sent_num += batch_size
    # read the whole table, with fake data
    # our mock server will decide what to send
    tablename = f"projects/project/instances/instance/tables/table"
    bt_request = ReadRowsRequest(table_name=tablename)
    request = test_proxy_pb2.ReadRowsRequest(client_id=client_id, request=bt_request)
    return request, server_response_fn


