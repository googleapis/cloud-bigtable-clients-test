import data_pb2
import bigtable_pb2
import test_proxy_pb2
import test_proxy_pb2_grpc as proxy_grpc
# import v2_test_proxy_pb2 as test_proxy_pb2 # legacy protos
# import v2_test_proxy_pb2_grpc as proxy_grpc # legacy protos
import grpc.experimental

def create_client(client_id, server_addr="localhost:9999", project_id="project", instance_id="instance"):
    # channel = grpc.secure_channel(server_addr, grpc.ssl_channel_credentials())

    proxy_client = proxy_grpc.CloudBigtableV2TestProxy()
    request = test_proxy_pb2.CreateClientRequest(
            client_id="test",
            data_target=server_addr,
            project_id=project_id,
            instance_id=instance_id,
    )
    response = proxy_client.CreateClient(request, server_addr, insecure=True)
    return response

def read_rows(server_addr="localhost:9999"):
    bt_request = bigtable_pb2.ReadRowsRequest(table_name="table", rows_limit=5)
    request = test_proxy_pb2.ReadRowsRequest(client_id="test", request=bt_request)
    proxy_client = proxy_grpc.CloudBigtableV2TestProxy()
    response = proxy_client.ReadRows(request, server_addr, insecure=True)
    return response

if __name__ == "__main__":
    r = read_rows()
    print(r)
