import timeit

import test_proxy_pb2
import test_proxy_pb2_grpc as proxy_grpc
from bigtable_pb2 import ReadRowsResponse, ReadRowsRequest
from google.protobuf.wrappers_pb2 import StringValue, BytesValue
from time import sleep

def simple_reads(client_id, proxy_addr, num_rows=1e7, payload_size=10, chunks_per_response=100, server_latency=0):
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
                    family_name=StringValue(value="F"),
                    qualifier=BytesValue(value=b"Q"), 
                    value=("a"*int(payload_size)).encode(),
                    commit_row=True
                ) for i in range(batch_size)
            ]
            sleep(server_latency)
            yield ReadRowsResponse(chunks=chunks)
            sent_num += batch_size

    def run_benchmark_fn():
        # read the whole table, with fake data
        # our mock server will decide what to send
        tablename = f"projects/project/instances/instance/tables/table"
        bt_request = ReadRowsRequest(table_name=tablename)
        request = test_proxy_pb2.ReadRowsRequest(client_id=client_id, request=bt_request)
        proxy_client = proxy_grpc.CloudBigtableV2TestProxy()

        starting_time = timeit.default_timer()
        grpc_options = (('grpc.max_receive_message_length', -1),)
        response = proxy_client.ReadRows(request, proxy_addr, options=grpc_options, insecure=True)
        print(f"Status: {response.status}\nRows: {len(response.row)}")
        diff = timeit.default_timer() - starting_time
        return diff

    return run_benchmark_fn, server_response_fn


