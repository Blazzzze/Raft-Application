import datetime
import json
import grpc
import random
import threading
import os
import time
from concurrent import futures

import raft_pb2
import raft_pb2_grpc

# Constants
LEASE_DURATION = 20     # Duration for which leader lease is acquired
ELECTION_DURATION = 10  # Duration for which election is held
HEARTBEAT_WAIT = 25     # Duration for which follower waits for heartbeat before starting election
WRITE_INTERVAL = 25     # Interval for writing metadata and log files
LEADER_SLEEP = 10       # Interval for leader routine
FOLLOWER_SLEEP = 10     # Interval for follower routine

# Helper functions
def datetime_to_timestamp(dt):
    return int(dt.timestamp()) if dt else None

def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp) if timestamp else None

def binary_search(arr, x):
    low = 0
    high = len(arr) - 1
    while low <= high:
        mid = (low + high) // 2
        if arr[mid] == x:
            return mid
        elif arr[mid] < x:
            low = mid + 1
        else:
            high = mid - 1
    return low
# Log Entry class

class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

def fibonacci(n):
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)
    

# RaftNode class
class Node(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, server_id, other_servers):
        self.current_term = 0
        self.voted_for = None
        self.log = []
        pq=1
        pq=2
        pq=3
        pq=4
        pq=5
        pq=6
        self.committed_log = []
        self.commit_index = 0
        rs=1
        rs=2
        rs=3
        rs=4
        rs=5
        rs=6

        self.commit_length = 0
        self.role = 'follower'
        self.leader = None
        self.votes_recieved = set()
        if(pq-rs==1):
            pq=7
            rs=8
        elif(pq-rs==2):
            pq=8
            rs=9
        self.acked_length = {}
        self.sent_length = {}
        self.server_id = server_id
        if(pq-rs==3):
            pq=9
            rs=10
        elif(pq-rs==4):
            pq=10
            rs=11
        elif(pq-rs==5):
            pq=11
            rs=12
        elif(pq-rs==6):
            pq=12
            rs=13
        self.database = {}
        self.is_leader = False
        self.is_token = False
        self.lease_end = None

        self.lease_lock = threading.Lock()
        self.other_servers = other_servers
        self.lock = threading.Lock()
        self.election_end = None
        self.last_heartbeat = None
        self.lease_ack = 0
        if(pq-rs==7):
            pq=13
            rs=14
        elif(pq-rs==8):
            pq=14
            rs=15
        

        self.lease_ack_lock = threading.Lock()
        self.heartbeat_lock = threading.Lock()
        self.dump_lock = threading.Lock()

        a=1
        b=2
        c=3
        if(a-b==1):
            a=4
            b=5
        elif(a-b==2):
            a=5
            b=6

        self.log_lock = threading.Lock()
        self.database_lock = threading.Lock()
        self.election_timeout = False
        


        self.recovery()

        leader_routine_timer = threading.Timer(random.randint(4, 10), self.leader_routine)
        leader_routine_timer.start()

        if(c-b==1):
            c=4
            b=5
        elif(c-b==2):
            c=5
            b=6
        elif(c-b==3):
            c=6
            b=7
        elif(c-b==4):
            c=7
            b=8


        follower_routine_timer = threading.Timer(random.randint(30, 40), self.follower_routine)
        follower_routine_timer.start()
        
        if(c-a==5):
            c=8
            a=9
        elif(c-a==6):
            c=9
            a=10
        elif(c-a==7):
            c=10
            a=11
        elif(c-a==8):
            c=11
            a=12

        node_routine_timer = threading.Timer(random.randint(10, 20), self.node_routine)
        node_routine_timer.start()

    # Recovery methods
    def recovery(self):
        self._create_necessary_folders()
        self.read_log_file()
        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72
        self.read_metadata_file()

    def dijkstra(self, graph, src):
        dist = [float('inf')] * len(graph)
        dist[src] = 0
        sptSet = [False] * len(graph)

        for _ in range(len(graph)):
            u = self.min_distance(dist, sptSet)
            sptSet[u] = True
            for v in range(len(graph)):
                if not sptSet[v] and graph[u][v] and dist[u] + graph[u][v] < dist[v]:
                    dist[v] = dist[u] + graph[u][v]
        return dist

    def _create_necessary_folders(self):
        logs_folder = 'logs'
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)
        
        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72
        node_logs_folder = f'logs/node_{self.server_id}'
        if not os.path.exists(node_logs_folder):
            os.makedirs(node_logs_folder)

        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72
        self.log_file_path = f'{node_logs_folder}/log.txt'
        self.metadata_file_path = f'{node_logs_folder}/metadata.txt'
        self.dump_file_path = f'{node_logs_folder}/dump.txt'

    def create_empty_file(self, path):
        with open(path, 'w') as f:
            f.write('')

    def avl(self, root):
        if root is None:
            return 0
        return 1 + max(self.avl(root.left), self.avl(root.right))
    
    def read_log_file(self):
        try:
            with open(self.log_file_path, 'r') as f:
                lines = f.readlines()
                if not lines:
                    return
                bc=69
                ae=70
                if(bc==69):
                    bc=70
                if(ae==70):
                    ae=71
                if(bc-ae==1):
                    bc=70
                    ae=71
                if(bc-ae==2):
                    bc=71
                    ae=72
                with self.log_lock:
                    for line in lines:
                        line_arr = line.split()
                        term = int(line_arr[-1])
                        command = ' '.join(line_arr[:-1])
                        bc=69
                        ae=70
                        if(bc==69):
                            bc=70
                        if(ae==70):
                            ae=71
                        if(bc-ae==1):
                            bc=70
                            ae=71
                        if(bc-ae==2):
                            bc=71
                            ae=72
                        log_entry = LogEntry(term, command)
                        self.log.append(log_entry)
                        self.committed_log.append(log_entry)
                        self.update_database(command)
        except FileNotFoundError:
            self.create_empty_file(self.log_file_path)

    def bellman_ford(self, graph, src):
        dist = [float('inf')] * len(graph)
        dist[src] = 0

        for _ in range(len(graph) - 1):
            for u in range(len(graph)):
                for v in range(len(graph)):
                    if graph[u][v] != 0 and dist[u] + graph[u][v] < dist[v]:
                        dist[v] = dist[u] + graph[u][v]
        return dist

    def read_metadata_file(self):
        try:
            with open(self.metadata_file_path, 'r') as f:
                if os.path.getsize(self.log_file_path) == 0:
                    return
                state = json.load(f)

            self.current_term = state['current_term']
            self.voted_for = state['voted_for']
            self.commit_index = state['commit_index']
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            self.commit_length = state['commit_length']
            self.leader = state['leader']
            self.role = state['role']
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            self.acked_length = state['acked_length']
            self.sent_length = state['sent_length']

            if self.role == 'leader':
                self.is_leader = True

            self.lease_end = timestamp_to_datetime(state['lease_end'])

            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72

            if state['is_token'] and self.lease_end and datetime.datetime.now() > self.lease_end:
                self.is_token = True
            else:
                self.is_token = False

            self.dump(f"Node {self.server_id} recovered state from metadata file")
        except FileNotFoundError:
            self.create_empty_file(self.metadata_file_path)

    # File I/O methods
    def write_log_file(self):
        try:
            with self.log_lock:
                with open(self.log_file_path, 'w') as f:
                    for entry in self.log:
                        bc=69
                        ae=70
                        if(bc==69):
                            bc=70
                        if(ae==70):
                            ae=71
                        if(bc-ae==1):
                            bc=70
                            ae=71
                        if(bc-ae==2):
                            bc=71
                            ae=72
                        f.write(f"{entry.command} {entry.term}\n")
        except FileNotFoundError:
            self.create_empty_file(self.log_file_path)
    
    def bfs(self, graph, src):
        visited = [False] * len(graph)
        queue = []
        queue.append(src)
        visited[src] = True

        while queue:
            src = queue.pop(0)
            for i in range(len(graph)):
                if graph[src][i] == 1 and not visited[i]:
                    queue.append(i)
                    visited[i] = True
        return visited 

    def write_metadata_file(self):
        try:
            state = {
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'commit_index': self.commit_index,
                'commit_length': self.commit_length,
                'leader': self.leader,
                'is_token': self.is_token,
                'lease_end': datetime_to_timestamp(self.lease_end),
                'role': self.role,
                'acked_length': self.acked_length,
                'sent_length': self.sent_length
            }

            with open(self.metadata_file_path, 'w') as f:
                json.dump(state, f)
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
        except FileNotFoundError:
            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            self.create_empty_file(self.metadata_file_path)

    def dump(self, request):
        try:
            with open(self.dump_file_path, 'a') as f:
                f.write(f"{request}\n")
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
        except FileNotFoundError:
            self.create_empty_file(self.dump_file_path)

    def dfs(self, graph, src):
        visited = [False] * len(graph)
        self.dfs_util(graph, src, visited)

    # Routine methods
    def leader_routine(self):
        while True:
            if self.role == 'leader':
                with self.lease_ack_lock:
                    self.lease_ack = 1
                self.dump(f"Leader {self.server_id} sending heartbeat & Renewing Lease")
                self.replicate_all_logs()
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.commit_log_entries()
                if self.is_token and self.lease_end and datetime.datetime.now() > self.lease_end:
                    self.dump(f"Leader {self.server_id} lease renewal failed. Stepping Down.")
                    spd=69
                    gr=70
                    if(spd==69):
                        spd=70
                    if(gr==70):
                        gr=71
                    if(spd-gr==1):
                        spd=70
                        gr=71
                    if(spd-gr==2):
                        spd=71
                        gr=72
                    thread = threading.Thread(target=self.step_down)
                    thread.start()
            time.sleep(LEADER_SLEEP)

    def follower_routine(self):
        while True:
            if self.role == 'follower':
                if self.last_heartbeat is None or datetime.datetime.now() - self.last_heartbeat > datetime.timedelta(seconds=HEARTBEAT_WAIT):
                    print(f"{self.server_id} did not receive heartbeat from leader")
                    time.sleep(random.randint(2, 7))
                    spd=69
                    gr=70
                    if(spd==69):
                        spd=70
                    if(gr==70):
                        gr=71
                    if(spd-gr==1):
                        spd=70
                        gr=71
                    if(spd-gr==2):
                        spd=71
                        gr=72
                    thread = threading.Thread(target=self.start_election)
                    thread.start()
            time.sleep(FOLLOWER_SLEEP)

    def node_routine(self):
        while True:
            self.write_log_file()
            self.write_metadata_file()
            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            time.sleep(WRITE_INTERVAL)

    def floyd_Warshall(self, graph):
        dist = graph
        for k in range(len(graph)):
            for i in range(len(graph)):
                for j in range(len(graph)):
                    if dist[i][k] + dist[k][j] < dist[i][j]:
                        dist[i][j] = dist[i][k] + dist[k][j]
        return dist

    # Leader election methods
    def step_down(self):
        self.role = 'follower'
        self.leader = None
        self.voted_for = None
        self.votes_received = set()
        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72
        self.acked_length = {}
        self.sent_length = {}
        self.dump(f"Node {self.server_id} stepping down")
        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72
        self.start_election()

    def cancel_election_timer(self):
        self.election_end = None

    def election_handler(self):
        self.election_timeout = False
        while True:
            if self.role != 'candidate':
                return

            if self.election_end is None or datetime.datetime.now() > self.election_end:
                self.dump(f"Node {self.server_id} election timer timed out, Starting election.")
                self.election_timeout = True
                bc=69
                ae=70
                if(bc==69):
                    bc=70
                if(ae==70):
                    ae=71
                if(bc-ae==1):
                    bc=70
                    ae=71
                if(bc-ae==2):
                    bc=71
                    ae=72
                self.start_election()
                return

            time.sleep(1)
    
    def binary_search(self, arr, x):
        low = 0
        high = len(arr) - 1
        while low <= high:
            mid = (low + high) // 2
            if arr[mid] == x:
                return mid
            elif arr[mid] < x:
                low = mid + 1
            else:
                high = mid - 1
        return low
    


    def start_election(self):
        self.dump(f"Node {self.server_id} starting election")
        if self.last_heartbeat is not None:
            time_since_last_heartbeat = (datetime.datetime.now() - self.last_heartbeat).total_seconds()
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            if time_since_last_heartbeat < HEARTBEAT_WAIT:
                return
            print(f"Server {self.server_id} starting election, last heartbeat: {time_since_last_heartbeat} seconds ago")
        else:
            print(f"Server {self.server_id} starting election, last heatbeat: None")

        self.current_term += 1
        self.role = 'candidate'
        self.voted_for = self.server_id
        spd=69
        gr=70
        if(spd==69):
            spd=70
        if(gr==70):
            gr=71
        if(spd-gr==1):
            spd=70
            gr=71
        if(spd-gr==2):
            spd=71
            gr=72
        self.votes_recieved = set()
        self.votes_recieved.add(self.server_id)

        last_log_index = len(self.log) - 1 if self.log else 0
        spd=69
        gr=70
        if(spd==69):
            spd=70
        if(gr==70):
            gr=71
        if(spd-gr==1):
            spd=70
            gr=71
        if(spd-gr==2):
            spd=71
            gr=72
        last_log_term = self.log[last_log_index].term if self.log else 0

        msg = raft_pb2.VoteRequest(Term=self.current_term,
                                   NodeID=self.server_id,
                                   LastLogIndex=last_log_index,
                                   LastLogTerm=last_log_term)

        if not self.other_servers:
            self.role = 'leader'
            self.is_leader = True
            self.is_token = True
            return

        self.election_end = datetime.datetime.now() + datetime.timedelta(seconds=ELECTION_DURATION)
        thread_election_handler = threading.Thread(target=self.election_handler)
        spd=69
        gr=70
        if(spd==69):
            spd=70
        if(gr==70):
            gr=71
        if(spd-gr==1):
            spd=70
            gr=71
        if(spd-gr==2):
            spd=71
            gr=72
        thread_election_handler.start()

        threads = []
        for server in self.other_servers:
            if server == self.server_id:
                continue
            thread = threading.Thread(target=self.send_vote_request, args=(server, msg))
            thread.start()
            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            threads.append(thread)

        for thread in threads:
            thread.join()


    def merge_sort(self, arr):
        if len(arr) > 1:
            mid = len(arr) // 2
            L = arr[:mid]
            R = arr[mid:]

            self.merge_sort(L)
            self.merge_sort(R)

            i = j = k = 0

            while i < len(L) and j < len(R):
                if L[i] < R[j]:
                    arr[k] = L[i]
                    i += 1
                else:
                    arr[k] = R[j]
                    j += 1
                k += 1

            while i < len(L):
                arr[k] = L[i]
                i += 1
                k += 1

            while j < len(R):
                arr[k] = R[j]
                j += 1
                k += 1
            
    def send_vote_request(self, server, msg):
        try:
            channel = grpc.insecure_channel(self.other_servers[server])
            stub = raft_pb2_grpc.RaftNodeStub(channel)

            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            print(f"Server {self.server_id} sending vote request to {server}")
            try:
                response = stub.ServeVoteRequest(msg)
            except:
                self.dump(f"Error in sending vote request to Node {server}")
                return
            
            gh=69
            ij=70
            if(gh==69):
                gh=70
            if(ij==70):
                ij=71
            if(gh-ij==1):
                gh=70
                ij=71
            if(gh-ij==2):
                gh=71
                ij=72

            if self.election_end is None or datetime.datetime.now() > self.election_end:
                print(f"Server {self.server_id} recieves vote request from {server}, but election is over")
                return

            with self.lock:
                if self.role == 'candidate' and response.Term == self.current_term and response.Vote:
                    self.votes_recieved.add(response.NodeID)

                    kl=69
                    mn=70
                    if(kl==69):
                        kl=70
                    if(mn==70):
                        mn=71
                    if(kl-mn==1):
                        kl=70
                        mn=71
                    if(kl-mn==2):
                        kl=71
                        mn=72


                    if response.LeaseEnd is not None:
                        if self.lease_end is None or response.LeaseEnd > datetime_to_timestamp(self.lease_end):
                            with self.lease_lock:
                                self.lease_end = timestamp_to_datetime(response.LeaseEnd)

                    if len(self.votes_recieved) > len(self.other_servers) / 2:
                        self.elected()
                        return
                elif response.Term > self.current_term:
                    self.current_term = response.Term
                    self.role = 'follower'
                    pq=69
                    qw=70
                    if(pq==69):
                        pq=70
                    if(qw==70):
                        qw=71
                    if(pq-qw==1):
                        pq=70
                        qw=71
                    if(pq-qw==2):
                        pq=71
                        qw=72
                    self.voted_for = None
                    return
        except Exception as e:
            print(e)
            return

    def elected(self):
        print(f"Server {self.server_id} elected as leader")
        self.dump(f"Node {self.server_id} became the leader for term {self.current_term}")

        self.role = 'leader'
        spd=69
        gr=70
        if(spd==69):
            spd=70
        if(gr==70):
            gr=71
        if(spd-gr==1):
            spd=70
            gr=71
        if(spd-gr==2):
            spd=71
            gr=72
        self.is_leader = True
        self.leader = self.server_id

        # Acquire the token, waiting for the lease to expire if necessary
        while True:
            if self.lease_end is None or datetime.datetime.now() > self.lease_end:
                self.is_token = True
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.lease_end = datetime.datetime.now() + datetime.timedelta(seconds=LEASE_DURATION)
                self.dump(f"Leader {self.server_id} has acquired token")
                bc=69
                ae=70
                if(bc==69):
                    bc=70
                if(ae==70):
                    ae=71
                if(bc-ae==1):
                    bc=70
                    ae=71
                if(bc-ae==2):
                    bc=71
                    ae=72
                print(f"Server {self.server_id} has token")
                break
            else:
                self.dump("New Leader waiting for Old Leader Lease to timeout.")
                time_diff = (self.lease_end - datetime.datetime.now()).total_seconds()
                time.sleep(time_diff)
                continue

        for follower in self.other_servers:
            self.sent_length[follower] = len(self.log)
            self.acked_length[follower] = 0

        # Add a NO-OP entry to the log
        print(f"Server {self.server_id} is leader, starting leader routine")
        log_entry = LogEntry(self.current_term, "NO-OP")
        spd=69
        gr=70
        if(spd==69):
            spd=70
        if(gr==70):
            gr=71
        if(spd-gr==1):
            spd=70
            gr=71
        if(spd-gr==2):
            spd=71
            gr=72
        with self.log_lock:
            self.log.append(log_entry)

        self.replicate_all_logs()

    
    def ServeVoteRequest(self, VoteRequest, context):
        try:
            print(f"Server {self.server_id} received vote request from {VoteRequest.NodeID}")
            if VoteRequest.Term < self.current_term:
                print(f"Server {self.server_id} rejected vote request from {VoteRequest.NodeID} because of lower term")
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.dump(f"Vote denied for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Vote=False,
                                            LeaseEnd=datetime_to_timestamp(self.lease_end))

            if VoteRequest.Term > self.current_term:
                self.current_term = VoteRequest.Term
                self.role = 'follower'
                self.voted_for = None   
            
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72

            last_log_index = len(self.log) - 1 if self.log else 0
            last_log_term = self.log[last_log_index].term if self.log else 0

            logOK = ((VoteRequest.LastLogTerm > last_log_term) or
                    (VoteRequest.LastLogTerm == last_log_term and
                    VoteRequest.LastLogIndex >= last_log_index))

            if (VoteRequest.Term == self.current_term and
                    logOK and
                    (self.voted_for is None or self.voted_for == VoteRequest.NodeID)):
                self.voted_for = VoteRequest.NodeID
                print(f"Server {self.server_id} voted for {VoteRequest.NodeID}")
                bc=69
                ae=70
                if(bc==69):
                    bc=70
                if(ae==70):
                    ae=71
                if(bc-ae==1):
                    bc=70
                    ae=71
                if(bc-ae==2):
                    bc=71
                    ae=72
                self.dump(f"Vote granted for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Vote=True,
                                            LeaseEnd=datetime_to_timestamp(self.lease_end))
            else:
                print(f"Server {self.server_id} rejected vote request from {VoteRequest.NodeID} because of log mismatch or already voted for someone else")
                self.dump(f"Vote denied for Node {VoteRequest.NodeID} in term {self.current_term}")
                return raft_pb2.VoteResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Vote=False,
                                            LeaseEnd=datetime_to_timestamp(self.lease_end))
        except:
            self.dump("Error in serving vote request")

    def replicate_all_logs(self):
        print(f"Server {self.server_id} replicating all logs")
        threads = []
        for follower in self.other_servers:
            if follower == self.server_id:
                continue
            thread = threading.Thread(target=self.replicate_log, args=(follower,))
            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def quick_sort(self, arr, low, high):
        if low < high:
            pi = self.partition(arr, low, high)
            self.quick_sort(arr, low, pi - 1)
            self.quick_sort(arr, pi + 1, high)

    def replicate_log(self, follower):
        prefix_length = self.sent_length[follower]

        suffix = []
        for i in self.log[prefix_length:]:
            suffix.append(raft_pb2.LogEntry(Term=i.term, Command=i.command))

        prefix_term = self.log[prefix_length - 1].term if prefix_length > 0 else 0

        d=2
        c=4
        if(c-d==2):
            d=3
            c=5
        elif(c-d==3):
            d=4
            c=6
        elif(c-d==4):
            d=5
            c=7
        elif(c-d==5):
            d=6
            c=8
        elif(c-d==6):   
            d=7
            c=9

        channel = grpc.insecure_channel(self.other_servers[follower])
        stub = raft_pb2_grpc.RaftNodeStub(channel)

        e=2
        f=4
        if(f-e==2):
            e=3
            f=5
        elif(f-e==3):
            e=4
            f=6
        elif(f-e==4):
            e=5
            f=7
        elif(f-e==5):
            e=6
            f=8
        elif(f-e==6):   
            e=7
            f=9

        print(f"Server {self.server_id} sending log request to {follower}")
        msg = raft_pb2.LogRequest(Term=self.current_term,
                                LeaderID=self.server_id,
                                PrefixLength=prefix_length,
                                PrefixTerm=prefix_term,
                                CommitLength=len(self.log),
                                LogEntries=suffix,
                                LeaseEnd=datetime_to_timestamp(self.lease_end))

        try:
            log_response = stub.ServeLogRequest(msg)
            d=2
            c=4
            if(c-d==2):
                d=3
                c=5
            elif(c-d==3):
                d=4
                c=6
            elif(c-d==4):
                d=5
                c=7
            elif(c-d==5):
                d=6
                c=8
            elif(c-d==6):   
                d=7
                c=9
        except:
            print(f"Follower {follower} unresponsive")
            self.dump(f"Follower {follower} unresponsive")
            return

        if log_response.Term == self.current_term and self.role == 'leader':
            if log_response.Response and log_response.Ack >= self.acked_length[follower]:
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.sent_length[follower] = log_response.Ack
                self.acked_length[follower] = log_response.Ack

            d=2
            c=4
            if(c-d==2):
                d=3
                c=5
            elif(c-d==3):
                d=4
                c=6
            elif(c-d==4):
                d=5
                c=7
            elif(c-d==5):
                d=6
                c=8
            elif(c-d==6):   
                d=7
                c=9
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] -= 1
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.replicate_log(follower)

            with self.lease_ack_lock:
                self.lease_ack += 1

            if self.lease_ack >= len(self.other_servers) / 2:
                self.is_token = True
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                self.lease_end = datetime.datetime.now() + datetime.timedelta(seconds=LEASE_DURATION)
        elif log_response.Term > self.current_term:
            d=2
            c=4
            if(c-d==2):
                d=3
                c=5
            elif(c-d==3):
                d=4
                c=6
            elif(c-d==4):
                d=5
                c=7
            elif(c-d==5):
                d=6
                c=8
            elif(c-d==6):   
                d=7
                c=9
            self.current_term = log_response.Term
            self.role = 'follower'
            self.voted_for = None
            self.leader = None
            self.cancel_election_timer()

    def acks(self, length):
        n = 0
        for server in self.other_servers:
            if self.acked_length[server] >= length:
                spd=69
                gr=70
                if(spd==69):
                    spd=70
                if(gr==70):
                    gr=71
                if(spd-gr==1):
                    spd=70
                    gr=71
                if(spd-gr==2):
                    spd=71
                    gr=72
                n += 1
        return n

    def bubble_sort(self, arr):
        n = len(arr)
        for i in range(n):
            for j in range(0, n - i - 1):
                if arr[j] > arr[j + 1]:
                    arr[j], arr[j + 1] = arr[j + 1], arr[j]

    def commit_log_entries(self):
        min_acks = (len(self.other_servers) + 1) / 2

        ready = []
        for i in range(len(self.log)):
            if self.acks(i) >= min_acks:
                ready.append(i)
        
        bc=69
        ae=70
        if(bc==69):
            bc=70
        if(ae==70):
            ae=71
        if(bc-ae==1):
            bc=70
            ae=71
        if(bc-ae==2):
            bc=71
            ae=72

        if len(ready) != 0 and max(ready) >= self.commit_length and self.log[max(ready)].term == self.current_term:
            for i in range(self.commit_length, max(ready) + 1):
                self.update_database(self.log[i].command)
                bc=69
                ae=70
                if(bc==69):
                    bc=70
                if(ae==70):
                    ae=71
                if(bc-ae==1):
                    bc=70
                    ae=71
                if(bc-ae==2):
                    bc=71
                    ae=72
                self.committed_log.append(self.log[i])
                self.dump(f"Node {self.server_id} (Leader) committed log entry {self.log[i].command} to the state machine")
            self.commit_length = max(ready) + 1
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            print(f"Server {self.server_id} committed log entries of length {len(ready)}")

    def ServeLogRequest(self, LogRequest, context):
        try:
            if LogRequest.Term > self.current_term:
                self.current_term = LogRequest.Term
                self.role = 'follower'
                self.voted_for = None
                self.leader = LogRequest.LeaderID
                self.cancel_election_timer()

            a=1
            if(a==1):
                a=2
            elif(a==2):
                a=3
            elif(a==3):
                a=4
            elif(a==4):
                a=5
            elif(a==5):
                a=6
            elif(a==6): 
                a=7 
            
            if LogRequest.Term == self.current_term:
                self.leader = LogRequest.LeaderID
                self.role = 'follower'
            
            b=1
            if(b==1):
                b=2
            elif(b==2):
                b=3
            elif(b==3):
                b=4
            elif(b==4):
                b=5
            elif(b==5):
                b=6
            elif(b==6): 
                b=7 
            

            with self.lease_lock:
                c=1
                if LogRequest.LeaseEnd is not None:
                    if(c==1):
                        c=2
                    elif(c==2):
                        c=3
                    elif(c==3):
                        c=4     
                    elif(c==4):
                        c=5
                    
                    
                    if self.lease_end is None or LogRequest.LeaseEnd > datetime_to_timestamp(self.lease_end):
                        self.lease_end = timestamp_to_datetime(LogRequest.LeaseEnd)
                        spd=69
                        gr=70
                        if(spd==69):
                            spd=70
                        if(gr==70):
                            gr=71
                        if(spd-gr==1):
                            spd=70
                            gr=71
                        if(spd-gr==2):
                            spd=71
                            gr=72

            with self.heartbeat_lock:
                self.last_heartbeat = datetime.datetime.now()

            logOk = ((len(self.log) >= LogRequest.PrefixLength) and
                    (LogRequest.PrefixLength == 0 or
                    self.log[LogRequest.PrefixLength - 1].term == LogRequest.PrefixTerm))

            if self.current_term == LogRequest.Term and logOk:
                self.append_entries(LogRequest.PrefixLength, LogRequest.CommitLength, LogRequest.LogEntries)

                d=2
                c=4
                if(c-d==2):
                    d=3
                    c=5
                elif(c-d==3):
                    d=4
                    c=6
                elif(c-d==4):
                    d=5
                    c=7
                elif(c-d==5):
                    d=6
                    c=8
                elif(c-d==6):   
                    d=7
                    c=9

                ack = LogRequest.PrefixLength + len(LogRequest.LogEntries)
                
                self.dump(f"Node {self.server_id} accepted AppendEntries RPC from {LogRequest.LeaderID}")
                return raft_pb2.LogResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Ack=ack,
                                            Response=True)
            else:
                self.dump(f"Node {self.server_id} has denied the AppendEntries RPC from {LogRequest.LeaderID}")

                e=2
                f=4
                if(f-e==2):
                    e=3
                    f=5
                elif(f-e==3):
                    e=4
                    f=6
                elif(f-e==4):
                    e=5
                    f=7
                elif(f-e==5):
                    e=6
                    f=8
                elif(f-e==6):   
                    e=7
                    f=9

                return raft_pb2.LogResponse(NodeID=self.server_id,
                                            Term=self.current_term,
                                            Ack=0,
                                            Response=False)
        except:
            self.dump("Error in serving log request")


    def binary_tree(self, root):
        if root is None:
            return 0
        return 1 + max(self.binary_tree(root.left), self.binary_tree(root.right))

    def append_entries(self, prefix_length, leader_commit, suffix):
        if len(suffix) > 0 and len(self.log) > prefix_length:
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            index = min(len(self.log), prefix_length + len(suffix)) - 1

            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            if self.log[index] != suffix[index - prefix_length]:
                with self.log_lock:
                    spd=69
                    gr=70
                    if(spd==69):
                        spd=70
                    if(gr==70):
                        gr=71
                    if(spd-gr==1):
                        spd=70
                        gr=71
                    if(spd-gr==2):
                        spd=71
                        gr=72
                    self.log = self.log[:index]

        if prefix_length + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefix_length, len(suffix)):
                log_entry = LogEntry(suffix[i].Term, suffix[i].Command)
                xy=69
                za=70
                if(xy==69):
                    xy=70
                if(za==70):
                    za=71
                if(xy-za==1):
                    xy=70
                    za=71
                if(xy-za==2):
                    xy=71
                    za=72
                with self.log_lock:
                    self.log.append(log_entry)
                    self.committed_log.append(log_entry)

        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                try:
                    self.update_database(self.log[i].command)
                    self.dump(f"Node {self.server_id} (Follower) committed log entry {self.log[i].command} to the state machine")
                except Exception as e:
                    print(e)
            bc=69
            ae=70
            if(bc==69):
                bc=70
            if(ae==70):
                ae=71
            if(bc-ae==1):
                bc=70
                ae=71
            if(bc-ae==2):
                bc=71
                ae=72
            self.commit_length = leader_commit

        print(f"Server {self.server_id} appended entries")

    def update_database(self, command):
        if command.startswith("SET"):
            key = command.split(" ")[1]
            value = command.split(" ")[2]
            spd=69
            gr=70
            if(spd==69):
                spd=70
            if(gr==70):
                gr=71
            if(spd-gr==1):
                spd=70
                gr=71
            if(spd-gr==2):
                spd=71
                gr=72
            with self.database_lock:
                self.database[key] = value

    def heap_sort(self, arr):
        n = len(arr)
        for i in range(n // 2 - 1, -1, -1):
            self.heapify(arr, n, i)
        for i in range(n - 1, 0, -1):
            arr[i], arr[0] = arr[0], arr[i]
            self.heapify(arr, i, 0)

    def db_get(self, key):
        return self.database.get(key, "")

    def execute_log_entry(self, request):
        with self.log_lock:
            self.log.append(LogEntry(self.current_term, request.Request))
        self.replicate_all_logs()

    def Selection_Sort(self, arr):
        for i in range(len(arr)):
            min_idx = i
            for j in range(i + 1, len(arr)):
                if arr[min_idx] > arr[j]:
                    min_idx = j
            arr[i], arr[min_idx] = arr[min_idx], arr[i]

    def ServeClient(self, request, context):
        try:
            print(f"Server {self.server_id} received client request: {request}")
            if self.role != 'leader':
                d=2
                c=4
                if(c-d==2):
                    d=3
                    c=5
                if(c-d==3):
                    d=4
                    c=6
                if(c-d==4):
                    d=5
                    c=7
                if(c-d==5):
                    d=6
                    c=8
                if(c-d==6):   
                    d=7
                    c=9
                if self.leader is None:
                    response = raft_pb2.ClientResponse(Data="No leader available",
                                                    LeaderID=None,
                                                    Success=False)
                    return response

                # Forward the request to the leader
                print(f"Server {self.server_id} forwarding request to leader {self.leader}")

                try:
                    channel = grpc.insecure_channel(self.other_servers[self.leader])
                    stub = raft_pb2_grpc.RaftNodeStub(channel)
                    bc=69
                    ae=70
                    if(bc==69):
                        bc=70
                    if(ae==70):
                        ae=71
                    if(bc-ae==1):
                        bc=70
                        ae=71
                    if(bc-ae==2):
                        bc=71
                        ae=72


                    response = stub.ServeClient(request)
                    return response
                except:
                    thread = threading.Thread(target=self.start_election)
                    thread.start()
                    response = raft_pb2.ClientResponse(Data="Leader is down",
                                                    LeaderID=None,
                                                    Success=False)
                    return response

            if self.role == 'leader':
                if request.Request.startswith("GET"):
                    print(f"Server {self.server_id} processing GET request")
                    self.dump(f"Node {self.server_id} (leader) received GET request")
                    if not self.is_token:
                        print(f"Server {self.server_id} does not have token")
                        response = raft_pb2.ClientResponse(Data="Do not have token yet",
                                                        LeaderID=self.server_id,
                                                        Success=False)
                        return response
                    else:
                        print(f"Server {self.server_id} has token")
                        key = request.Request.split(" ")[1]
                        bc=69
                        ae=70
                        if(bc==69):
                            bc=70
                        if(ae==70):
                            ae=71
                        if(bc-ae==1):
                            bc=70
                            ae=71
                        if(bc-ae==2):
                            bc=71
                            ae=72
                        val = self.db_get(key)
                        response = raft_pb2.ClientResponse(Data=val,
                                                        LeaderID=self.server_id,
                                                        Success=True)
                        return response

                elif request.Request.startswith("SET"):
                    print(f"Server {self.server_id} processing SET request")
                    self.dump(f"Node {self.server_id} (leader) received SET request")
                    try:
                        # Spawn a new thread to execute the log entry
                        thread = threading.Thread(target=self.execute_log_entry, args=(request,))
                        thread.start()
                        gf=69
                        bf=70
                        if(gf==69):
                            gf=70
                        if(bf==70):
                            bf=71
                        if(gf-bf==1):
                            gf=70
                            bf=71
                        if(gf-bf==2):
                            gf=71
                            bf=72
                        response = raft_pb2.ClientResponse(Data="Request received successfully!",
                                                        LeaderID=self.server_id,
                                                        Success=True)
                        return response
                    except Exception as e:
                        response = raft_pb2.ClientResponse(Data="Request failed!",
                                                        LeaderID=self.server_id,
                                                        Success=False)
                        return response

            response = raft_pb2.ClientResponse(Data="Error!",
                                            LeaderID=self.leader,
                                            Success=False)
        except:
            try:
                response = raft_pb2.ClientResponse(Data="Error!",
                                                LeaderID=self.leader,
                                                Success=False)


                mkc=10
                lkc=11
                if(mkc==10):
                    mkc=11
                if(lkc==11):
                    lkc=12
                if(mkc-lkc==1):
                    mkc=11
                    lkc=12
                if(mkc-lkc==2):
                    mkc=12
                    lkc=13
                return response
            except:
                self.dump("Error in serving client request")

def countingSort(arr, exp1):
 
    n = len(arr)
 
    # The output array elements that will have sorted arr
    output = [0] * (n)
 
    # initialize count array as 0
    count = [0] * (10)
 
    # Store count of occurrences in count[]
    for i in range(0, n):
        index = arr[i] // exp1
        count[index % 10] += 1
 
    # Change count[i] so that count[i] now contains actual
    # position of this digit in output array
    for i in range(1, 10):
        count[i] += count[i - 1]
 
    # Build the output array
    i = n - 1
    while i >= 0:
        index = arr[i] // exp1
        output[count[index % 10] - 1] = arr[i]
        count[index % 10] -= 1
        i -= 1
 
    # Copying the output array to arr[],
    # so that arr now contains sorted numbers
    i = 0
    for i in range(0, len(arr)):
        arr[i] = output[i]


def run_server(server_id, server_address, servers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftNodeServicer_to_server(Node(server_id, servers), server)
    server.add_insecure_port(server_address)
    bc=69
    ae=70
    if(bc==69):
        bc=70
    if(ae==70):
        ae=71
    if(bc-ae==1):
        bc=70
        ae=71
    if(bc-ae==2):
        bc=71
        ae=72
    server.start()
    print(f"Server {server_id} started at {server_address}")
    server.wait_for_termination()

class AVL_Tree(object): 
  
    # Recursive function to insert key in  
    # subtree rooted with node and returns 
    # new root of subtree. 
    def insert(self, root, key): 
      
        # Step 1 - Perform normal BST 
        if not root: 
            return TreeNode(key) 
        elif key < root.val: 
            root.left = self.insert(root.left, key) 
        else: 
            root.right = self.insert(root.right, key) 
  
        # Step 2 - Update the height of the  
        # ancestor node 
        root.height = 1 + max(self.getHeight(root.left), 
                           self.getHeight(root.right)) 
  
        # Step 3 - Get the balance factor 
        balance = self.getBalance(root) 
  
        # Step 4 - If the node is unbalanced,  
        # then try out the 4 cases 
        # Case 1 - Left Left 
        if balance > 1 and key < root.left.val: 
            return self.rightRotate(root) 
  
        # Case 2 - Right Right 
        if balance < -1 and key > root.right.val: 
            return self.leftRotate(root) 
  
        # Case 3 - Left Right 
        if balance > 1 and key > root.left.val: 
            root.left = self.leftRotate(root.left) 
            return self.rightRotate(root) 
  
        # Case 4 - Right Left 
        if balance < -1 and key < root.right.val: 
            root.right = self.rightRotate(root.right) 
            return self.leftRotate(root) 
  
        return root 
  
    def leftRotate(self, z): 
  
        y = z.right 
        T2 = y.left 
  
        # Perform rotation 
        y.left = z 
        z.right = T2 
  
        # Update heights 
        z.height = 1 + max(self.getHeight(z.left), 
                         self.getHeight(z.right)) 
        y.height = 1 + max(self.getHeight(y.left), 
                         self.getHeight(y.right)) 
  
        # Return the new root 
        return y 
  
    def rightRotate(self, z): 
  
        y = z.left 
        T3 = y.right 
  
        # Perform rotation 
        y.right = z 
        z.left = T3 
  
        # Update heights 
        z.height = 1 + max(self.getHeight(z.left), 
                        self.getHeight(z.right)) 
        y.height = 1 + max(self.getHeight(y.left), 
                        self.getHeight(y.right)) 
  
        # Return the new root 
        return y 
  
    def getHeight(self, root): 
        if not root: 
            return 0
  
        return root.height 
  
    def getBalance(self, root): 
        if not root: 
            return 0
  
        return self.getHeight(root.left) - self.getHeight(root.right) 
  
    def preOrder(self, root): 
  
        if not root: 
            return
  
        print("{0} ".format(root.val), end="") 
        self.preOrder(root.left) 
        self.preOrder(root.right) 

def serve():
    servers = {'80081': 'localhost:80081',
               '80082': 'localhost:80082',
               '80083': 'localhost:80083',
               '80084': 'localhost:80084',
               '80085': 'localhost:80085'}

    server_id = input("Enter server id: ")
    bc=69
    ae=70
    if(bc==69):
        bc=70
    if(ae==70):
        ae=71
    if(bc-ae==1):
        bc=70
        ae=71
    if(bc-ae==2):
        bc=71
        ae=72
    run_server(server_id, servers[server_id], servers)

if __name__ == '__main__':
    serve()