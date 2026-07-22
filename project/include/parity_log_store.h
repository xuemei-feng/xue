#ifndef ECPROJECT_PARITY_LOG_STORE_H
#define ECPROJECT_PARITY_LOG_STORE_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace ECProject
{
  struct ParityLogAppendResult
  {
    bool ok = false;
    bool need_d0 = false;
    bool log_merged = false;
  };

  class ParityLogStore
  {
  public:
    static ParityLogStore &instance();

    ParityLogAppendResult append_new_data(int stripe_id, int parity_block_id, const std::string &parity_block_key,
                                          int data_block_id, int range_offset, int range_length, int k, int r, int z,
                                          int block_size, const std::string &code_type, const char *new_data,
                                          int new_data_len);

    bool store_d0(int stripe_id, int parity_block_id, int data_block_id, int range_offset, int range_length,
                  const char *d0, int d0_len);

    bool merge_if_full_cached(int stripe_id, int parity_block_id, int datanode_port);

    bool clear_stripe_logs(int stripe_id, int parity_begin, int parity_end);

    bool merge_if_full(int stripe_id, int parity_block_id, const std::string &parity_block_storage_path, int k, int r,
                       int z, int block_size, const std::string &code_type);

  private:
    struct IntervalKey
    {
      int data_block_id = -1;
      int range_offset = 0;
      int range_length = 0;
      bool operator<(const IntervalKey &o) const;
    };

    struct LogEntry
    {
      uint64_t seq = 0;
      bool has_d0 = false;
      std::vector<char> d0;
      std::vector<char> new_data;
    };

    struct ParityLog
    {
      std::map<IntervalKey, LogEntry> entries;
      uint64_t used_bytes = 0;
      uint64_t next_seq = 1;
      std::string parity_block_key;
      int k = 0;
      int r = 0;
      int z = 0;
      int block_size = 0;
      std::string code_type;
      std::mutex mu;
    };

    ParityLog &log_for(int stripe_id, int parity_block_id);
    static uint64_t entry_bytes(const LogEntry &e);
    static uint64_t complete_bytes_locked(const ParityLog &log);
    static void recompute_used_bytes_locked(ParityLog &log);
    bool merge_log_locked(ParityLog &log, int stripe_id, int parity_block_id, const std::string &parity_block_storage_path,
                          int k, int r, int z, int block_size, const std::string &code_type);

    std::mutex map_mu_;
    std::map<std::pair<int, int>, ParityLog> logs_;
  };
} // namespace ECProject

#endif
