import grpc
import raft_pb2
import raft_pb2_grpc
import random
import time

class Client:
    def __init__(self, servers):
        self.servers = servers
        self.leader_id = random.choice(list(self.servers.keys()))

    def validate_request(self, request):
        request_parts = request.split()
        if len(request_parts) not in (2,3):
            return False
        if request_parts[0] not in ('GET', 'SET'):
            return False
        if len(request_parts) == 3 and request_parts[0] == 'GET':
            return False
        if len(request_parts) == 2 and request_parts[0] == 'SET':
            return False
        return True

    def get_new_leader(self):
        self.leader_id = random.choice(list(self.servers.keys()))

    def handle_response(self, response):
        if response.Success:
            print("Request sent successfully!")
            print("Response from server: ", response.Data)
            if response.LeaderID:
                self.leader_id = response.LeaderID
        else:
            if response.Data in ["Leader is down", "No leader available"]:
                print(response.Data, ". Trying another server.")
                self.get_new_leader()
                time.sleep(10)
            elif response.Data == "Invalid request!":
                print("Invalid request!")
            else:
                if response.LeaderID:
                    self.leader_id = response.LeaderID

    def send_request(self, request):
        if not self.validate_request(request):
            print("Invalid request!")
            return

        while True:
            try:
                client_request = raft_pb2.ClientRequest(Request=request)
                channel = grpc.insecure_channel(self.servers[self.leader_id])
                stub = raft_pb2_grpc.RaftNodeStub(channel)
                try:
                    response = stub.ServeClient(client_request)
                    self.handle_response(response)
                    break
                except grpc.RpcError as e:
                    print(self.leader_id, " is down. Trying another server.")
                    self.get_new_leader()
            except Exception as e:
                print("Error. Please retry.")
                break

if __name__ == '__main__':
    servers = {'50051':'localhost:50051',
               '50052': 'localhost:50052',
               '50053': 'localhost:50053',
               '50054': 'localhost:50054',
               '50055': 'localhost:50055',
               }
    client = Client(servers)
    while True:
        query = input("Enter your query: ")
        client.send_request(query)