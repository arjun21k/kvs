#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_SERVER_H_
#define MICA_DATAGRAM_DATAGRAM_SERVER_H_

#include "mica/common.h"
#include <thread>
#include "mica/util/config.h"
#include "mica/processor/partitions.h"
#include "mica/network/dpdk.h"
#include "mica/directory/directory_client.h"
#include "mica/util/stopwatch.h"
#include "mica/datagram/datagram_protocol.h"

#include <unistd.h>
#include <time.h>
#include <iostream>
#include <csignal>
#include <mutex>
//#include <sys/select.h>

#include <doca_argp.h>
#include <doca_dev.h>
#include <doca_log.h>
#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_mmap.h>
#include "mica/doca_util/utils.h"
#include "mica/doca_util/dma_common.h"

DOCA_LOG_REGISTER(DMA_COPY_HOST::SERVER);

#define MAX_DMA_BUF_SIZE (1024 * 1024)  /* DMA buffer maximum size */
#define RECV_BUF_SIZE 256               /* Buffer which contains config information */

// Configuration file entries for DatagramServer:
//
//  * directory_refresh_interval (integer): The time interval in seconds to
//    refresh the server information on the directory.  This should be smaller
//    than TTL used by DirectoryClient. Default = 1
//  * directory_refresh_lcore (integer): The ID of the lcore to use refreshing
//    the directory.  Default = (the last lcore ID in the system)
//  * rebalance_interval (integer): The time interval in seconds to rebalance
//    the load.  Use 0 to disable rebalancing.  Default = 0
//  * flush_status_report (bool): Flush the regular status output.  Default =
//    true

namespace mica {
namespace datagram {
struct BasicDatagramServerConfig {
  // The processor type.
  typedef ::mica::processor::Partitions<> Processor;

  // The network type.
  typedef ::mica::network::DPDK<> Network;

  // The maximum number of LCore to support.
  // static constexpr size_t kMaxLCoreCount = 64;

  // The RX burst size.
  static constexpr uint16_t kRXBurst = 64;

  // The TX burst size.
  static constexpr uint16_t kTXBurst = 64;

  // The TX burst size to flush.  Must be no larger than kTXBurst.
  static constexpr uint16_t kTXMinBurst = 1;

  // The TX burst timeout (accumulation time) in microseconds.
  static constexpr uint16_t kTXBurstTimeout = 10;

  // The maximum number of parsed requests.  This must be at least as large as
  // the maximum number of requests per a request batch (packet) AND the
  // maximum number of requests the processor prefetches in advance.  It must
  // be a power of two.
  static constexpr size_t kMaxParsedRequestCount = 128;

  // Be verbose.
  static constexpr bool kVerbose = false;
};

template <class StaticConfig = BasicDatagramServerConfig>
class DatagramServer {
 public:
  typedef typename StaticConfig::Processor Processor;
  typedef typename StaticConfig::Network Network;
  typedef ::mica::directory::DirectoryClient DirectoryClient;

  //DatagramServer(const ::mica::util::Config& config, Processor* processor,
  //               Network* network, DirectoryClient* dir_client);
  DatagramServer(const ::mica::util::Config& config, Processor* processor, Network* network); 
  ~DatagramServer();

  void run();

  static void sig_handler(int signum);

 private:
  ::mica::util::Config config_;
  Processor* processor_;
  Network* network_;
  DirectoryClient* dir_client_;

  typedef ::mica::processor::Operation Operation;
  typedef ::mica::table::Result Result;

  typedef typename Network::EndpointId EndpointId;
  typedef typename Network::PacketBuffer PacketBuffer;

  struct RXTXState {
    EndpointId eid;

    struct PendingTX {
      std::array<PacketBuffer*, StaticConfig::kTXBurst> bufs;
      uint16_t count;
      uint64_t oldest_time;
    } pending_tx;
  };

  struct WorkerStats {
    uint64_t alive;
    uint64_t num_operations_done;
    uint64_t num_operations_succeeded;

    uint64_t last_num_operations_done;
    uint64_t last_num_operations_succeeded;
  } __attribute__((aligned(128)));

  struct EndpointStats {
    uint64_t last_rx_bursts;
    uint64_t last_rx_packets;
    uint64_t last_tx_bursts;
    uint64_t last_tx_packets;
    uint64_t last_tx_dropped;
  };

  struct RequestHeader_1 {
    // 0
    uint8_t operation;  // ::mica::processor::Operation
    uint8_t result;     // ::mica::table::Result
    // 2
    uint16_t reserved0;
    // 4
    uint32_t kv_length_vec;  // key_length: 8, value_length: 24
    // 8
    uint64_t key_hash;
    // 16
    uint32_t opaque;
    uint32_t reserved1;
    // 24
    uint64_t timestamp;
    // 32
    // Key-value data
    uint64_t value;
  };

  struct MiniResponse {
    uint64_t value;
    uint32_t value_length;
    uint8_t result;
    uint16_t reserved0;
    uint8_t reserved1;
  };

  // Directory service support.
  void generate_server_info();
  static void directory_proc_wrapper(void* arg);
  void directory_proc();

  // Doca
  doca_error_t doca_init(int lcore_id);
  doca_error_t save_config_info_to_files(char *export_desc, size_t export_desc_len, const char *src_buffer, size_t src_buffer_len, char *export_desc_file_path, char *buffer_info_file_path);
  doca_error_t save_config_info_to_buffers(const char *export_desc_file_path, const char *buffer_info_file_path, char *export_desc, size_t *export_desc_len, char **remote_addr, size_t *remote_addr_len);
  doca_error_t doca_send_buf_init(int lcore_id);
  uint8_t check_dma_recv_buf_for_requests(size_t lcore_id);
  uint8_t check_dma_recv_buf_for_requests_1();

  // Main worker.
  static int worker_proc_wrapper(void* arg);
  void worker_proc(uint16_t lcore_id);

  // TX packet handling.
  void check_pending_tx_full(RXTXState& tx_state);
  void check_pending_tx_min(RXTXState& tx_state);
  void check_pending_tx_timeout(RXTXState& tx_state, uint64_t now);
  void release_pending_tx(RXTXState& tx_state);

  // Diagnosis.
  void reset_status();
  void report_status(double time_diff);

  // Request accessor to supply the request processor with requests.
  class RequestAccessor : public ::mica::processor::RequestAccessorInterface {
   public:
    RequestAccessor(DatagramServer<StaticConfig>* server,
                    WorkerStats* worker_stats, uint16_t lcore_id);
    ~RequestAccessor();

    void setup(RXTXState* rx_tx_state,
               //std::array<PacketBuffer*, StaticConfig::kRXBurst>* bufs,
               size_t packet_count, uint64_t now, uint16_t lcore_id);

    bool prepare(size_t index);
    Operation get_operation(size_t index);
    uint64_t get_key_hash(size_t index);
    uint64_t get_request_timestamp(size_t index);
    const char* get_key(size_t index);
    size_t get_key_length(size_t index);
    const char* get_value(size_t index);
    size_t get_value_length(size_t index);
    char* get_out_value(size_t index);
    size_t get_out_value_length(size_t index);
    void set_out_value_length(size_t index, size_t len);
    void set_result(size_t index, Result result);
    void retire(size_t index);

    void append_response_to_dma_buf(size_t index, Result result, size_t value_length);
    doca_error_t doca_dma_write(uint16_t lcore_id);
    void flush_pending_response_batch_from_host_send_buf();
    void print_response_in_host_dma_buf(size_t index);

   private:
    static constexpr size_t kMaxParsedRequestCount =
        StaticConfig::kMaxParsedRequestCount;
    static constexpr size_t kParsedRequestMask = kMaxParsedRequestCount - 1;

    DatagramServer<StaticConfig>* server_;
    WorkerStats* worker_stats_;
    uint16_t lcore_id_;

    RXTXState* rx_tx_state_;
    std::array<PacketBuffer*, StaticConfig::kRXBurst>* bufs_;
    size_t packet_count_;
    uint64_t now_;

    struct ParsedRequest {
      // Members are ordered by size for tight packing.
      PacketBuffer* src_buf;
      const char* key;
      const char* value;
      uint64_t key_hash;
      uint64_t timestamp;
      uint32_t value_length;
      uint32_t opaque;
      uint8_t key_length;
      uint8_t operation;
      uint8_t last_in_packet;
    };

    struct ParsedRequest_1 {
      // Members are ordered by size for tight packing.
      PacketBuffer* src_buf;
      uint64_t key;
      uint64_t value;
      uint64_t key_hash;
      uint64_t timestamp;
      uint32_t value_length;
      uint32_t opaque;
      uint8_t key_length;
      uint8_t operation;
      uint8_t last_in_packet;
    };

    struct MiniParsedReq {
      uint64_t key;
      uint64_t value;
      uint64_t key_hash;
      uint32_t value_length;
      uint8_t key_length;
      uint8_t operation;
    };

    struct PendingResponseBatch {
      RequestBatchBuilder<PacketBuffer> b;

      // These members store values from RequestAccessor::set_*() temporarily
      // for the next index to retire.
      size_t value_length;
      Result result;
    };

    bool parse_request_batch();
    uint32_t get_opaque(size_t index);

    // Pending request batch management.
    void make_new_pending_response_batch();
    void release_pending_response_batch();
    void flush_pending_response_batch(const PacketBuffer* src_buf);

    std::array<ParsedRequest, kMaxParsedRequestCount> requests_;
    PendingResponseBatch pending_response_batch_;

    uint16_t next_packet_index_to_parse_;
    uint16_t next_index_to_prepare_;
    uint16_t next_index_to_retire_;  // Used for diagnosis only.
  };

  ::mica::util::Stopwatch stopwatch_;

  uint32_t directory_refresh_interval_;
  uint16_t directory_refresh_lcore_;

  uint32_t rebalance_interval_;

  bool flush_status_report_;

  std::string server_info_;
  std::thread directory_thread_;
  volatile bool stopping_;

  // Padding to separate static and dynamic fields.
  char padding0[128];

  std::vector<WorkerStats> worker_stats_;

  // Padding to separate two dynamic fields.
  char padding1[128];

  std::vector<EndpointStats> endpoint_stats_;
};
}
}

#include "mica/datagram/datagram_server_impl.h"

#endif
