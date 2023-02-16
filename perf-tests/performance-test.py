import data_pb2
import bigtable_pb2
# import test_proxy_pb2
# import test_proxy_pb2_grpc as proxy_grpc
import v2_test_proxy_pb2 as test_proxy_pb2
import v2_test_proxy_pb2_grpc as test_proxy_grpc
import grpc.experimental

def create_client(client_id, server_addr="localhost:9999", project_id="project", instance_id="instance"):
    # channel = grpc.secure_channel(server_addr, grpc.ssl_channel_credentials())

    proxy_client = test_proxy_grpc.CloudBigtableV2TestProxy()
    request = test_proxy_pb2.CreateClientRequest(
            client_id="test",
            data_target=server_addr,
            project_id=project_id,
            instance_id=instance_id,
    )
    response = proxy_client.CreateClient(request, server_addr, insecure=True)
    return response

def read_row():
    pass

if __name__ == "__main__":
    c = create_client("sanche")
    print(c)
