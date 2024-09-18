//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <linux/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/blockingconcurrentqueue.h"
#include "rocksdb/concurrentqueue.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

#define PACK_TO_BUF(src, buf, len) \
  {                                \
    memcpy((buf), (src), (len));   \
    (buf) += (len);                \
  }
#define UNPACK_FROM_BUF(buf, des, len) \
  {                                    \
    memcpy((des), (buf), (len));       \
    (buf) += (len);                    \
  }

struct imm_read_req_v2 {
  // derived from imm_read_req
  int32_t status_code;
  char key[25];  // lookup key
  size_t memtable_key_len;
  bool found_final_value;
  uint64_t max_covering_tombstone_seq;
  bool allow_data_in_errors;
  size_t protection_bytes_per_key;
  int32_t timestamp_size_;  // usercomparator timestamp size
  char timestamp[25];
  uint64_t seq;
  // global info
  size_t mixed_ids_size;
  uint64_t mixed_ids[25];
};

struct imm_read_req {
  int32_t status_code;
  char key[25];  // lookup key
  size_t memtable_key_len;
  bool found_final_value;
  uint64_t max_covering_tombstone_seq;
  bool allow_data_in_errors;
  size_t protection_bytes_per_key;
  uint64_t mixed_id;        // memtable identifier
  int32_t timestamp_size_;  // usercomparator timestamp size
  char timestamp[25];
  uint64_t seq;
};
struct imm_read_ret {
  int32_t status_code;
  char value[300];
  size_t value_size;
  char timestamp[25];
  int32_t timestamp_size;
  bool found_final_value;
  uint64_t seq;
  int64_t offset;
  uint64_t mem_id;
};

// a mempool, using malloc/free to allocate/free memorys
class RegularMemNode {
  std::vector<std::pair<void *, size_t>> mempool_;
  std::mutex mtx_;

 public:
  RegularMemNode() = default;
  ~RegularMemNode() {
    for (auto &p : mempool_) {
      ::free(reinterpret_cast<char *>(p.first));
    }
    mempool_.clear();
  }
  char *allocate(size_t size) {
    std::lock_guard<std::mutex> lock(mtx_);
    char *addr = reinterpret_cast<char *>(malloc(size));
    mempool_.emplace_back(addr, size);
    return addr;
  }
  void free(char *addr) {
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto it = mempool_.begin(); it != mempool_.end(); ++it) {
      if (it->first == addr) {
        ::free(addr);
        mempool_.erase(it);
        break;
      }
    }
  }
};
// a mempool, using registered buffer
class RDMAMemNode {
  std::set<std::pair<uint64_t, uint64_t>> mempool_;
  std::mutex mtx_;
  uint64_t buf_size_;

 public:
  RDMAMemNode() = default;
  ~RDMAMemNode() { mempool_.clear(); }
  void init(uint64_t buf_size) { buf_size_ = buf_size; }
  int64_t allocate(uint64_t size) {
    while (true) {
      std::lock_guard<std::mutex> lock(mtx_);
      bool flag = false;
      uint64_t offset = 0;
      if (mempool_.empty()) {
        if (size <= buf_size_)
          offset = 0, flag = true;
        else {
          fprintf(stderr, "Memory node does not have enough capacity\n");
          return -1;
        }
      } else {
        if (size <= mempool_.begin()->first)
          offset = 0, flag = true;
        else
          for (auto i = mempool_.begin(); i != mempool_.end(); i++) {
            auto j = i;
            j++;
            if (i->first + i->second + size <=
                (j != mempool_.end() ? j->first : buf_size_)) {
              offset = i->first + i->second, flag = true;
              break;
            }
          }
      }
      if (flag) {
        mempool_.insert(std::make_pair(offset, size));
        return offset;
      }
    }
  }
  void free(uint64_t offset) {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = mempool_.lower_bound(std::make_pair(offset, 0));
    assert(it != mempool_.end());
    assert(it->first == offset);
    mempool_.erase(it);
  }
};

// tcp node, use in flush_job_server & worker & memnode.
// use sockfd to exchange data with peer
class TCPNode {
 public:
  explicit TCPNode(sockaddr_in client_addr, int32_t client_sockfd)
      : connection_info_{client_addr, client_sockfd, 0} {}
  ~TCPNode() {}
  bool send(const void *buf, size_t size);

  // set address & length to avoid unnecessary copy
  bool receive(void *buf, size_t size) {
    assert(buf != nullptr);
    return receive(&buf, size);
  }
  // set length, buffer will be allocated/released by TCPNode::mempool
  bool receive(void **buf, size_t size) {
    assert(size != 0);
    return receive(buf, &size);
  }
  // data length not sure
  bool receive(void **buf, size_t *size);

  struct tcp_connect_meta {
    struct sockaddr_in sin_addr;
    int32_t client_sockfd;
    int32_t listen_sockfd;
  } __attribute__((aligned)) connection_info_;

 private:
  RegularMemNode memory_;
};

struct placement_info {
  int current_background_job_num_ = 0;
  int current_hdfs_io_ = 0;
};

// PD listen on port 10086, receive FlushRequest from generator, receive
// HeartBeat from worker

struct PlacementDriver;
class RDMANode {
  friend class PlacementDriver;
  friend class RDMAServer;
  friend class RDMAClient;
  friend class RDMAReadClient;
  friend class RemoteFlushJob;
  friend class DBImpl;

 public:
  // structure of test parameters
  struct config_t {
    std::string dev_name;  // IB device name
    int ib_port;           // local IB port to work with
    int gid_idx;           // gid index to use
    int max_cqe, max_send_wr, max_recv_wr;
  };
  // structure to exchange data which is needed to connect the QPs
  struct cm_con_data_t {
    uint64_t addr;    // Buffer address
    uint32_t rkey;    // Remote key
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
  } __attribute__((packed));
  struct rdma_connection {
    struct ibv_cq *cq;  // CQ handle
    struct ibv_qp *qp;  // QP handle
    int sock;           // TCP socket file descriptor
    sockaddr_in addr;   // tcp connection address for name resolution
  };
  // structure of system resources
  struct resources {
    struct ibv_device_attr device_attr;  // Device attributes
    struct ibv_port_attr port_attr;      // IB port attributes
    struct cm_con_data_t remote_props;   // values to connect to remote side
    struct ibv_context *ib_ctx;          // device handle
    struct ibv_pd *pd;                   // PD handle
    std::vector<struct rdma_connection *> conns;
    struct ibv_mr *mr;  // MR handle for buf
    char *buf;          // memory buffer pointer, used for RDMA and send ops
  };

 private:
  struct rdma_connection *connect_qp(int sock);
  int sock_sync_data(int sock, int xfer_size, const char *local_data,
                     char *remote_data);
  int post_send(struct rdma_connection *idx, size_t msg_size,
                ibv_wr_opcode opcode, long long local_offset,
                long long remote_offset, uint64_t wr_id = 0);
  int post_receive(struct rdma_connection *idx, size_t msg_size,
                   long long local_offset, uint64_t wr_id = 0);
  int modify_qp_to_init(struct ibv_qp *qp);
  int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn, uint16_t dlid,
                       uint8_t *dgid);
  int modify_qp_to_rts(struct ibv_qp *qp);
  int resources_destroy();
  virtual void after_connect_qp(struct rdma_connection *idx) = 0;
  std::unique_ptr<std::mutex> conns_mtx;

 public:
  RDMANode();
  virtual ~RDMANode() = 0;
  int resources_create(size_t size);
  struct rdma_connection *sock_connect(const std::string &server_name = "",
                                       u_int32_t tcp_port = 9091);
  int send(struct rdma_connection *idx, size_t msg_size, long long local_offset,
           uint64_t wr_id = 0) {
    return post_send(idx, msg_size, IBV_WR_SEND, local_offset, 0, wr_id);
  }
  int receive(struct rdma_connection *idx, size_t msg_size,
              long long local_offset, uint64_t wr_id = 0) {
    return post_receive(idx, msg_size, local_offset, wr_id);
  }
  int rdma_read(struct rdma_connection *idx, size_t msg_size,
                long long local_offset, long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_READ, local_offset,
                     remote_offset);
  }
  int rdma_write(struct rdma_connection *idx, size_t msg_size,
                 long long local_offset, long long remote_offset) {
    return post_send(idx, msg_size, IBV_WR_RDMA_WRITE, local_offset,
                     remote_offset);
  }
  int poll_completion(struct rdma_connection *idx);
  char *get_buf() { return res->buf; }
  struct resources *res;
  size_t buf_size;
  struct config_t config;
};
class RDMAReadClient : public RDMANode {
  // note: one read client should only use one rdma_connection, data structure
  // below is actually for rdma_connection
 public:
  std::vector<ibv_wc *> rr_wc_buf;
  std::atomic_int32_t pending_rr_num{0};
  moodycamel::BlockingConcurrentQueue<size_t> available_read_reqs_;  // 16

 private:
  void after_connect_qp(struct rdma_connection *idx) override {}

 public:
  RDMAMemNode rdma_mem_;
  RDMAReadClient();
  ~RDMAReadClient() override;
  int rr_block_poll_completion(struct rdma_connection *conn,
                               uint64_t wr_id = 0);
  bool register_client_in_get_service_request(struct rdma_connection *conn,
                                              bool v2 = false);
  bool client_send_request_for_memtable_read_v2(
      struct rdma_connection *conn,
      imm_read_req_v2 *req_packet /* to be updated*/, std::string *value,
      std::string *timestamp);
  bool disconnect_request(struct rdma_connection *conn);
};

struct PlacementDriver {
  const int max_background_job_num_{32};
  const int max_hdfs_io_{100 << 20};  // MB/s
  std::queue<TCPNode *> available_workers_;
  std::vector<TCPNode *> workers_;
  std::vector<TCPNode *> generators_;
  std::unordered_map<TCPNode *, placement_info> peers_;
  TCPNode *choose_worker(const placement_info &);
  struct RDMANode::rdma_connection *choose_worker_rdma(const placement_info &);
  void step(bool, size_t, placement_info);
  void poll_events(int port);
};
class RemoteMemTablePool;
class RDMAServer : public RDMANode {
  struct executor_info {
    int status;
    moodycamel::BlockingConcurrentQueue<std::pair<int64_t, int64_t>>
        flush_job_queue;
    std::pair<int64_t, int64_t> current_job;
  };

 public:
  RDMAServer();
  ~RDMAServer() override;
  bool service(struct rdma_connection *idx);
  void connect_clients(int port) {
    std::thread listen_thread{[this, port]() { pd_.poll_events(port); }};
    listen_thread.detach();
  }

 private:
  int rr_block_poll_completion(struct rdma_connection *conn,
                               std::vector<ibv_wc *> *rr_wc_buf,
                               uint64_t wr_id = 0,
                               bool *should_close = nullptr);
  void create_rmem_service(struct rdma_connection *idx);
  void receive_rmem_service(struct rdma_connection *idx);
  void receive_remote_flush_service(struct rdma_connection *idx,
                                    int64_t &meta_offset, int64_t &meta_size);
  void allocate_mem_service(struct rdma_connection *idx, int64_t &ret_offset,
                            int64_t &size);
  void free_mem_service(struct rdma_connection *conn);
  void disconnect_service(struct rdma_connection *idx);
  void register_executor_service(struct rdma_connection *idx);
  void wait_for_job_service(struct rdma_connection *idx);
  void register_memtable_read_service(struct rdma_connection *idx,
                                      std::thread *t, bool *should_close);
  void fetch_memtable_service(struct rdma_connection *conn);
  void register_client_in_get_service_service(
      struct rdma_connection *conn, std::vector<size_t> *delegated_read_buffer_,
      std::vector<std::thread> *delegated_read_threads_,
      moodycamel::BlockingConcurrentQueue<uint64_t> *wr_info_,
      std::vector<ibv_wc *> *rr_wc_buf, bool *should_close);
  void register_client_in_get_service_service_v2(
      struct rdma_connection *conn, std::vector<size_t> *delegated_read_buffer_,
      std::vector<std::thread> *delegated_read_threads_,
      moodycamel::BlockingConcurrentQueue<uint64_t> *wr_info_,
      std::vector<ibv_wc *> *rr_wc_buf, bool *should_close);
  RemoteMemTablePool *remote_memtable_pool_;
  std::unique_ptr<std::mutex> mempool_mtx;
  std::set<std::pair<int64_t /*offset*/, int64_t /*len*/>> pinned_mem;
  inline int64_t pin_mem(int64_t size) {
    std::lock_guard<std::mutex> lck(*mempool_mtx);
    int64_t last_end = 0;
    for (auto iter : pinned_mem) {
      int64_t front_len = iter.first - last_end;
      if (front_len >= size) {
        pinned_mem.insert(std::make_pair(last_end, size));
        return last_end;
      } else {
        last_end = iter.first + iter.second;
      }
    }
    if (buf_size - last_end >= size) {
      pinned_mem.insert(std::make_pair(last_end, size));
      return last_end;
    }
    return -1;
  }
  inline bool unpin_mem(int64_t offset, int64_t size) {
    std::lock_guard<std::mutex> lck(*mempool_mtx);
    auto iter = pinned_mem.find(std::make_pair(offset, size));
    if (iter != pinned_mem.end()) {
      pinned_mem.erase(iter);
      return true;
    }
    return false;
  }

  std::vector<std::thread *> threads;
  std::unordered_map<struct rdma_connection *, executor_info> executors_;
  void after_connect_qp(struct rdma_connection *idx) override {
    auto ser = [this, idx] { service(idx); };
    threads.push_back(new std::thread(ser));  // each connection
    threads.back()->detach();
  }
  PlacementDriver pd_;
  struct rdma_connection *choose_flush_job_executor(
      const std::pair<int64_t, int64_t> &job_mem_tobe_registered);
};

class RDMAClient : public RDMANode {
 public:
  RDMAClient();
  ~RDMAClient() override = default;
  std::pair<int64_t, int64_t> allocate_mem_request(struct rdma_connection *idx,
                                                   int64_t size);
  // qry_type:
  // type == 0 client_request_disconnect
  // type == 1 client_send_memtable
  // type == 2 client_send_metadata_for_remote_flush
  // type == 3 client_recv_memtable_for_local_flush
  // type == 4 client_send_request_for_memtable_read
  // type == 5 memnode_send_package_for_remote_flush
  // type == fd memnode_recv_installinfo_from_worker
  // type == fe client_recv_installinfo_for_remote_flush
  // type == ff client_send_sgn_flush_calculation

  bool disconnect_request(struct rdma_connection *idx);
  void free_mem_request(struct rdma_connection *idx, int64_t offset,
                        int64_t size);  // req_type=2
  bool register_executor_request(struct rdma_connection *idx);
  void fetch_memtable_request(
      struct rdma_connection *conn, uint64_t mixed_id, void *&index,
      uint64_t &index_size, void *&meta, uint64_t &mem_size,
      std::pair<void *, uint64_t> *mem_data);  // req_type=10
  std::pair<int64_t, int64_t> wait_for_job_request(struct rdma_connection *idx);
  size_t port = -1;
  RegularMemNode memory_;
  RDMAMemNode rdma_mem_;

 private:
  void after_connect_qp(struct rdma_connection *idx) override {}
};

// register_workers then opentcp
class RemoteFlushJobPD {
 public:
  RemoteFlushJobPD(const RemoteFlushJobPD &) = delete;
  auto operator=(const RemoteFlushJobPD &) -> RemoteFlushJobPD & = delete;
  static RemoteFlushJobPD &Instance() {
    static RemoteFlushJobPD instance;
    return instance;
  }

 public:
  bool opentcp(int port, int heartbeat_port);
  bool closetcp();
  void register_flush_job_generator(int fd, const TCPNode *node) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (flush_job_generators_.find(fd) != flush_job_generators_.end())
      assert(false);
    flush_job_generators_.insert(
        std::make_pair(fd, const_cast<TCPNode *>(node)));
  }
  TCPNode *unregister_flush_job_generator(int fd) {
    std::lock_guard<std::mutex> lock(mtx_);
    assert(flush_job_generators_.find(fd) != flush_job_generators_.end());
    TCPNode *node = flush_job_generators_.at(fd);
    flush_job_generators_.erase(fd);
    return node;
  }
  void register_flush_job_executor([[maybe_unused]] const std::string &ip,
                                   int port) {
    std::lock_guard<std::mutex> lock(mtx_);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    assert(inet_pton(AF_INET, ip.data(), &serv_addr.sin_addr) > 0);
    serv_addr.sin_port = htons(port);

    auto *node = new TCPNode(serv_addr, {});
    flush_job_executors_status_.insert(std::make_pair(node, true));
  }
  void unregister_flush_job_executor(TCPNode *node) {
    std::lock_guard<std::mutex> lock(mtx_);
    flush_job_executors_status_.erase(node);
    assert(flush_job_executors_in_use_.find(
               node->connection_info_.client_sockfd) ==
           flush_job_executors_in_use_.end());
    assert(node->connection_info_.client_sockfd <= 0);
    delete node;
  }
  void poll_events(int port) { pd_.poll_events(port); }

 private:
  struct flushjob_package {
    std::vector<std::pair<void *, size_t>> package;
  };
  flushjob_package *receive_remote_flush_job(TCPNode *generator_node);
  TCPNode *choose_flush_job_executor();
  void setfree_flush_job_executor(TCPNode *worker_node);
  void send_remote_flush_job(flushjob_package *package, TCPNode *worker_node);

  std::mutex mtx_;
  std::unordered_map<int, TCPNode *> flush_job_executors_in_use_;
  std::unordered_map<TCPNode *, bool> flush_job_executors_status_;
  std::unordered_map<int, TCPNode *> flush_job_generators_;
  PlacementDriver pd_;
  struct tcp_server_info {
    int tcp_server_sockfd_;
    struct sockaddr_in server_address;
  } __attribute__((aligned)) server_info_ = {-1};

 private:
  RemoteFlushJobPD() = default;
  ~RemoteFlushJobPD() {}
};

class PDClient {
 public:
  TCPNode *pd_connection_{nullptr};

 private:
  mutable std::mutex pd_mutex_;
  size_t peer_id_{0};
  int heartbeatport_{10086};
  std::function<placement_info()> get_placement_info{
      []() { return placement_info(); }};

 public:
  explicit PDClient(int heartbeatport) : heartbeatport_(heartbeatport) {}
  inline std::mutex &get_mutex() const { return pd_mutex_; }
  inline void set_get_placement_info(
      const std::function<placement_info()> &func) {
    get_placement_info = func;
  }
  inline Status MatchMemnodeForHeartBeat(const std::string &ip,
                                         bool is_worker) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      assert(false);
      return Status::IOError("socket error");
    }
    sockaddr_in addr_;
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(heartbeatport_);
    addr_.sin_addr.s_addr = inet_addr(ip.data());
    printf("rocksdb local_heartbeatport_ %d\n", heartbeatport_);

    if (connect(sock, (struct sockaddr *)&addr_, sizeof(addr_)) < 0) {
      fprintf(stderr, "connect error\n");
      close(sock);
      return Status::IOError("connect error");
    }

    struct sockaddr_in client_address;
    socklen_t client_address_len = sizeof(client_address);
    if (getsockname(sock, reinterpret_cast<sockaddr *>(&client_address),
                    &client_address_len) == -1) {
      std::cerr << "Failed to get local address." << std::endl;
      return Status::IOError("getsockname error");
    } else {
      unsigned short port = ntohs(client_address.sin_port);
      std::cerr << "Connected to memnode. Local port: " << port << std::endl;
    }
    {
      char client_ip[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &client_address.sin_addr, client_ip, INET_ADDRSTRLEN);
      int client_port = ntohs(client_address.sin_port);
      LOG_CERR("Rocksdb Instance create connection with memnode: ", client_ip,
               ':', client_port);
    }
    auto *node = new TCPNode(client_address, sock);
    pd_connection_ = node;
    pd_connection_->send(&is_worker, sizeof(bool));
    pd_connection_->receive(&peer_id_, sizeof(size_t));
    return Status::OK();
  }
  inline Status SendHeartBeat(const placement_info &pinfo) {
    {
      std::lock_guard<std::mutex> lock(pd_mutex_);
      if (pd_connection_ == nullptr) {
        assert(false);
        return Status::IOError("pd_connection_ is nullptr");
      }
      pd_connection_->send(&pinfo, sizeof(placement_info));
    }
    return Status::OK();
  }
  inline void match_memnode_for_request(const std::string &ip) {
    Status s = MatchMemnodeForHeartBeat(ip, false);
    assert(s.ok());
  }
  inline void match_memnode_for_heartbeat(const std::string &ip) {
    Status s = MatchMemnodeForHeartBeat(ip, true);
    assert(s.ok());
    std::thread heartbeat_thread([this]() {
      placement_info lastpinfo;
      int cnt = 0;
      while (true) {
        placement_info pinfo = get_placement_info();
        if (lastpinfo.current_background_job_num_ !=
                pinfo.current_background_job_num_ ||
            lastpinfo.current_hdfs_io_ != pinfo.current_hdfs_io_ || cnt >= 3) {
          lastpinfo = pinfo;
          cnt = 0;
          Status s0 = SendHeartBeat(pinfo);
          if (!s0.ok()) break;
        } else {
          cnt++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    });
    heartbeat_thread.detach();
  }
  inline void register_memnode_heartbeat_port(int port = 10086) {
    heartbeatport_ = port;
  }
};

}  // namespace ROCKSDB_NAMESPACE