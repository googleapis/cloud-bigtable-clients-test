import data_pb2
import bigtable_pb2
import bigtable_pb2_grpc
import test_proxy_pb2
import test_proxy_pb2_grpc as proxy_grpc
# import v2_test_proxy_pb2 as test_proxy_pb2 # legacy protos
# import v2_test_proxy_pb2_grpc as proxy_grpc # legacy protos
import grpc.experimental
import concurrent.futures as futures
import benchmark

def create_client(client_id, server_addr, proxy_addr, project_id="project", instance_id="instance"):
    """
    server_addr: the address of the mock server running in this process
    proxy_addr: the address of the client library proxy running in a different process
    """

    proxy_client = proxy_grpc.CloudBigtableV2TestProxy()
    request = test_proxy_pb2.CreateClientRequest(
            client_id=client_id,
            data_target=server_addr,
            project_id=project_id,
            instance_id=instance_id,
    )
    response = proxy_client.CreateClient(request, proxy_addr, insecure=True)
    return response

class MockBigtableServicer(bigtable_pb2_grpc.BigtableServicer):

    def __init__(self, serve_fn):
        self.serve_fn = serve_fn

    def ReadRows(self, request, context):
        yield from self.serve_fn()


def serve(server_addr="localhost:8081", serve_fn=None):
  print("starting server")
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  bigtable_pb2_grpc.add_BigtableServicer_to_server(
       MockBigtableServicer(serve_fn=serve_fn), server)
  server.add_insecure_port(server_addr)
  server.start()
  return server

if __name__ == "__main__":
    proxy_addr = "localhost:9999"
    server_addr = "localhost:8888"

    client_id = "test_client"

    benchmark_request, benchmark_serve_fn = benchmark.simple_reads(client_id, proxy_addr)

    server = serve(server_addr=server_addr, serve_fn=benchmark_serve_fn)
    c = create_client(client_id, server_addr=server_addr, proxy_addr=proxy_addr)

    time = benchmark_request()
    print(time)
    # server.wait_for_termination()
