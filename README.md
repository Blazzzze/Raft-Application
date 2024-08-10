# Raft

This project focuses on implementing a modified Raft system with the leader lease modification, similar to those used by geo-distributed database clusters such as [CockroachDB](https://www.cockroachlabs.com/) or [YugabyteDB](https://www.yugabyte.com/).

Raft is a consensus algorithm designed for distributed systems to ensure fault tolerance and consistency. It operates through leader election, log replication, and commitment of entries across a cluster of nodes. The aim is to build a database that stores key-value pairs mapping string (key) to string (value).

## Table of Contents

- [About The Project](#about-the-project)
  - [Built With](#built-with)
- [Documentation](#documentation)
  - [Storage and Database Options](#storage-and-database-options)
  - [Client Interaction](#client-interaction)
  - [RPC and Protobuf](#rpc-and-protobuf)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Deployment](#deployment)
  - [Remote Deployment](#remote-deployment)
  - [Local Deployment](#local-deployment)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)

## About The Project

This project implements a distributed key-value store using the Raft consensus algorithm with leader lease modifications to optimize read performance in geo-distributed environments.

### Built With

- [Python](https://www.python.org/)
- [gRPC](https://grpc.io/)
- [Google Cloud](https://cloud.google.com/)

## Documentation

### Storage and Database Options

The Raft nodes serve the client for storing and retrieving key-value pairs and replicating this data across nodes to ensure fault tolerance.

- **Persistence:** Logs and metadata are stored persistently in human-readable formats (`.txt` files) and are retrieved when the node restarts.
- **Directory Structure:** 
  - Each node has its own directory for storing logs, metadata, and a dump file, such as:
    ```
    logs_node_1/
    ├── logs.txt
    ├── metadata.txt
    ├── dump.txt
    ```
- **Supported Operations:**
  - `SET K V`: Maps the key `K` to value `V`.
  - `GET K`: Returns the latest committed value of key `K`.

### Client Interaction

The Raft cluster serves clients by processing their `SET` and `GET` requests through the leader node. If a request fails, the client updates its leader information and retries.

### RPC and Protobuf

The project uses gRPC for communication between nodes and between the client and nodes. The primary RPCs implemented are `AppendEntry` and `RequestVote`, modified to include leader lease duration for optimizing reads.

## Getting Started

### Prerequisites

- Python ([Download](https://www.python.org/downloads/))
- gRPC ([Installation Guide](https://grpc.io/docs/languages/python/quickstart/))

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/Raft.git
2. Navigate to the project directory:
   ```sh
   cd Raft
3. Install the required dependencies:
   ```sh
   pip install -r requirements.txt

## Deployment

### Remote Deployment

To deploy the Raft cluster on Google Cloud or Docker containers:

1. Set up the necessary virtual machines or containers on Google Cloud.

2. Update the IP addresses and ports in node.py and client.py to reflect the correct networking configuration for remote deployment.

3. Grant execution permissions to the deployment script and run it:
   ```sh
   chmod +x scripts/deploy_remote.sh
   ./scripts/deploy_remote.sh

### Local Deployment

For local deployment on your machine:

1. Ensure all the nodes and the client are configured with localhost IP addresses.

2. Grant execution permissions to the local deployment script and run it:
   ```sh
   chmod +x scripts/deploy_local.sh
   ./scripts/deploy_local.sh

## Roadmap
 * Implement basic Raft algorithm
 
 * Add leader lease modification for optimized read performance
 
 * Implement dynamic cluster management (adding/removing nodes)
 
 * Improve fault tolerance mechanisms
 
 * Comprehensive testing and benchmarking

## Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are greatly appreciated.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Don't forget to give the project a star! Thanks again!

1. Fork the Project

2. Create your Feature Branch (git checkout -b feature/AmazingFeature)

3. Commit your Changes (git commit -m 'Add some AmazingFeature')

4. Push to the Branch (git push origin feature/AmazingFeature)

5. Open a Pull Request

## Acknowledgements

* Aryan Rohilla - aryan21024@iiitd.ac.in

* Ankit Kumar - ankit21015@iiitd.ac.in

* Dharani Kumar S - dharani21039@iiitd.ac.in
