## Prototype

The architecture follows master-worker style, like many state-of-art distributed file storage such as HDFS and Ceph. Four major components are client, coordinator, proxy and datanode. 

### Environment Configuration

- Required packages

  * grpc v1.50

  * asio 1.24.0

  * jerasure

- we call them third_party libraries, and the source codes are provided in the `third_party/` directory.

- Before installing these packages, you should install the dependencies of grpc.

  - ```
    sudo apt install -y build-essential autoconf libtool pkg-config
    ```

- Run the following command to install these packages

  - ```
    sh install_third_party.sh
    ```

### System Configuration

- Cluster configuration. The cluster configuration is defined in `clusterInformation.xml`, which contains the following fields:

  * `cluster id`: The unique identifier for each cluster
  * `proxy`: The IP address and port number of the proxy server for this cluster
  * `datanodes`: A list of datanode configurations for this cluster
    - `uri`: The IP address and port number of each datanode

- System parameter configuration. The system parameters are defined in `parameterConfiguration.xml`, which contains the following fields:

  * `AlignedSize`: The size in bytes that data should be aligned to (4096 bytes)
  * `UnitSize`: The basic unit size for data operations (8192 bytes)
  * `BlockSize`: The size of data blocks (16384 bytes)
  * `DatanodeNumPerCluster`: Number of datanodes in each cluster (8)
  * `ClusterNum`: Total number of clusters in the system (6)
  * `CoordinatorIP`: IP address of the coordinator server (0.0.0.0)
  * `CoordinatorPort`: Port number for the coordinator server (55555)
  * `AppendMode`: The mode for append operations, can be:
    - REP_MODE: Replication mode
    - UNILRC_MODE: Uniform LRC mode  
    - CACHED_MODE: Cached mode
  * `alpha`: Parameter for coding (1)
  * `CodeType`: Type of erasure coding scheme, can be:
    - UniLRC: UniLRC
    - AzureLRC: Azure LRC
    - OptimalLRC: Optimal LRC
    - UniformLRC: Uniform LRC
  * `k`: Number of data blocks
  * `r`: Number of global parity blocks
  * `z`: Number of local parity blocks

### Compile and Run

- Compile

```
sh compile.sh
```

- Run

```
# Run proxy and datanode
sh start_proxy.sh

# Run coordinator
sh start_coordinator.sh

# Run client
sh test.sh
```

#### Attention

- In `parameterConfiguration.xml`, if `CodeType` is UniLRC, the `k`, `r` is computed based on `alpha` and `z`; if `CodeType` is AzureLRC, OptimalLRC or UniformLRC, the `k`, `r`, and `z` are directly specified.
- start_proxy.sh and start_coordinator.sh scripts need adjustment for different environments.



### Other

#### Directory

- directory `doc/`  is the introduction of system implementation.
- directory `project/` is the system implementation.
- create directory `storage/` to store the data blocks for data nodes.
- create directory `run_cluster_sh/` to store the running shell for each cluster.

#### Tools

- use `small_tools/generator_sh.py` to generate configuration file and running shell for proxy and data node.

