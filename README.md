# **Dream-LSM**

## **Dependencies**

- Linux - Ubuntu 18.04
- Prepare for the dependencies of RocksDB based on this [link](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)
- Install and config HDFS server as the plugin of RocksDB
- Install RDMA packages

## Deployment
### **Dream-LSM**
- Notice that multiple clients should bind with one server.

- **DM management**:

  1. Config your HDFS cluster:
  In the env_posix.cc, line 427:
  `Status s = NewHdfsFileSystem("hdfs://hdfs-master:9000/", &fs);`
  Change this based on your hdfs cluster address.

  2. Config your server address:
  In the column_family.cc
  `std::string memnode_ip = "10.10.1.3";`
  Change this based on your DM management address

  3. Build and compile

    ```
    git checkout server
    mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make rdma_server
    ./rdma_server
    ```

- **Dream-LSM instance**
  1. Config your HDFS cluster:
  In the env_posix.cc, line 427:
  `Status s = NewHdfsFileSystem("hdfs://hdfs-master:9000/", &fs);`
  Change this based on your hdfs cluster address.
  2. Config your server address:
  In the column_family.cc
  `std::string memnode_ip = "10.10.1.3";`
  Change this based on your DM management address
  3. Build and compile
  
  ```
  git checkout client
  mdkir build
  cd build
  cmake -DCMAKE_BUILD_TYPE=Release ..
  make remote_flush_worker
  ./remote_flush_worker [server ip][server port][local listen port]
  ```


## Baselines



### **Disaggre-RocksDB**

- Install RocksDB
- Install and config HDFS as the plugin of RocksDB
- Refer to this [link](https://github.com/riversand963/rocksdb-hdfs-env)

### **CaaS-LSM**

- CaaS-LSM is a state-of-the-art implementation for remote
compaction on disaggregated storage.

- Refer to this [link](https://github.com/asd1ej9h/CaaS-LSM)

### **Nova-LSM**

- Nova-LSM is an optimized LSM-tree for storage disaggregation (instead of memory disaggregation).

- Refer to this [link](https://github.com/HaoyuHuang/NovaLSM)


## Evaluation

Run db_bench with Dream-LSM

```
./db_bench --num=1845000 --max_write_buffer_number=8 --disable_auto_compactions=1 --cache_index_and_filter_blocks=true --pin_l0_filter_and_index_blocks_in_cache=true --bloom_bits=10 --cache_size=536870912 --memtable_bloom_size_ratio=0.1 --memtable_whole_key_filtering=true --max_local_write_buffer_number=2 --max_background_compactions=4 --max_background_flushes=4 --level0_slowdown_writes_trigger=32 --level0_stop_writes_trigger=48 --benchmarks=fillrandom,stats --key_size=16 --value_size=64 --num_column_families=1 --threads=16 --write_buffer_size=67108864 --use_remote_flush=1 --min_write_buffer_number_to_merge=8 --subcompactions=4 --compaction_readahead_size=10485760 --disable_wal=1 --db=/tmp/test --memnode_heartbeat_port=10086 --report_fillrandom_latency_and_load=true --track_flush_compaction_stats=true --statistics=true --memnode_ip=10.10.1.7 --memnode_port=9091 --local_ip=10.10.1.4 --compression_type=none
```
