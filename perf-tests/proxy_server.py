# Copyright 2015 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC server."""

import logging
from multiprocessing import Process
import multiprocessing
import time


def proxy_server_process(q):
    from concurrent import futures

    import grpc
    import test_proxy_pb2
    import test_proxy_pb2_grpc

    # from proxy_server_2 import CreateClient as proxy_client

    class TestProxyServer(test_proxy_pb2_grpc.CloudBigtableV2TestProxyServicer):

        def CreateClient(self, request, context):
            return test_proxy_pb2.CreateClientResponse()


        def CloseClient(self, request, context):
            print("close the client")
            print(f"{request=}")
            return test_proxy_pb2.CloseClientResponse()

        def ReadRow(self, request, context):
            print(f"readrow: {request=}")
            return test_proxy_pb2.RowResult()

        def ReadRows(self, request, context):
            # print(f"read rows: {request.client_id=} {request.request=}" )
            q.put(request.request.SerializeToString())
            return test_proxy_pb2.RowsResult()

        def MutateRow(self, request, context):
            print(f"mutate rows: {request.client_id=} {request.request=}" )
            return test_proxy_pb2.MutateRowResult()

        def RemoveClient(self, request, context):
            print(f"removeclient request {request.client_id=}")
            print(request)
            return test_proxy_pb2.RemoveClientResponse()

    port = '50055'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_proxy_pb2_grpc.add_CloudBigtableV2TestProxyServicer_to_server(TestProxyServer(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()

def client_process(q):
    import google.cloud.bigtable_v2 as bigtable_v2
    print("listening")
    while True:
        if not q.empty():
            print("got something")
            data = q.get()
            request = bigtable_v2.ReadRowsRequest.deserialize(data)
            print(request)
        else:
            print("nothing")
            time.sleep(1)

if __name__ == '__main__':
    q = multiprocessing.Queue()
    logging.basicConfig()
    proxy = Process(target=proxy_server_process, args=(q,))
    proxy.start()
    client = Process(target=client_process, args=(q,))
    client.start()
    proxy.join()
    client.join()
