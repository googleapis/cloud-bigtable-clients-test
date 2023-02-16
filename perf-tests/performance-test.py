import data_pb2
import bigtable_pb2
import bigtable_pb2_grpc
import test_proxy_pb2
import test_proxy_pb2_grpc as proxy_grpc
# import v2_test_proxy_pb2 as test_proxy_pb2 # legacy protos
# import v2_test_proxy_pb2_grpc as proxy_grpc # legacy protos
import grpc.experimental
import concurrent.futures as futures

def create_client(client_id, mock_server_addr="localhost:8081", server_addr="localhost:9999", project_id="project", instance_id="instance"):
    # channel = grpc.secure_channel(server_addr, grpc.ssl_channel_credentials())

    proxy_client = proxy_grpc.CloudBigtableV2TestProxy()
    request = test_proxy_pb2.CreateClientRequest(
            client_id=client_id,
            data_target=mock_server_addr,
            project_id=project_id,
            instance_id=instance_id,
    )
    response = proxy_client.CreateClient(request, server_addr, insecure=True)
    return response

def read_rows(server_addr="localhost:9999"):
    tablename = f"projects/project/instances/instance/tables/table"
    bt_request = bigtable_pb2.ReadRowsRequest(table_name=tablename, rows_limit=5)
    request = test_proxy_pb2.ReadRowsRequest(client_id="test", request=bt_request)
    proxy_client = proxy_grpc.CloudBigtableV2TestProxy()
    response = proxy_client.ReadRows(request, server_addr, insecure=True)
    return response

class MockBigtableServicer(bigtable_pb2_grpc.BigtableServicer):
    def ReadRows(self, request, context):
        for i in range(5):
            yield bigtable_pb2.ReadRowsResponse(chunks=[])


def serve():
  print("starting server")
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  bigtable_pb2_grpc.add_BigtableServicer_to_server(
       MockBigtableServicer(), server)
  server.add_insecure_port('[::]:8081')
  server.start()
  return server

if __name__ == "__main__":
    server = serve()
    c = create_client("test")
    print(f"c = {c}")
    r = read_rows()
    print(f"r = {r}")
    server.wait_for_termination()
