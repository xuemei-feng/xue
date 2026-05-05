#ifndef ECPROJECT_PARIX_JOURNAL_H
#define ECPROJECT_PARIX_JOURNAL_H

#include "config.h"
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace ECProject
{

  /** Per-proxy Parix journal: partial writes append (d_r, optional d_0); replay applies p += a_ij*(d_r XOR d_0). */
  class ParixJournal
  {
  public:
    struct Entry
    {
      uint64_t batch_id = 0;
      uint64_t write_generation = 0;
      int parity_block_id = -1;
      std::string parity_block_key;
      std::string parity_datanode_ip;
      int parity_datanode_port = 0;
      int data_block_id = -1;
      int range_offset = 0;
      int len = 0;
      std::string new_payload;
      std::string old_payload;
      bool has_old = false;
      bool replayed = false;
    };

    struct StripeState
    {
      uint64_t write_generation = 1;
      std::vector<Entry> entries;
      /** After replay, mirrors committed d_r per slice so later appends may skip NEED_D0 when in order with commit. */
      std::map<std::tuple<int, int, int>, std::string> last_committed_dr;
    };

    enum class AppendResult
    {
      SUCCESS,
      NEED_D0
    };

    AppendResult append(int stripe_id, uint64_t batch_id, uint64_t write_generation, int parity_block_id,
                        const std::string &parity_block_key, const std::string &parity_datanode_ip, int parity_datanode_port,
                        int data_block_id, int range_offset, int len, const std::string &new_payload);

    bool supply_d0(int stripe_id, uint64_t batch_id, uint64_t write_generation, int parity_block_id,
                   int data_block_id, int range_offset, int len, const std::string &old_payload);

    /** Apply all completed entries for batch_id on this stripe; updates parity blocks via callbacks. */
    bool replay_batch(int stripe_id, uint64_t batch_id, Config *cfg,
                      const std::function<bool(const std::string &key, int block_id, char *buf, size_t bs, const char *ip, int port)> &read_parity,
                      const std::function<bool(const std::string &key, int block_id, const char *buf, size_t bs, const char *ip, int port)> &write_parity);

    /** Remove journal entries (and last_committed_dr hints) whose data slice intersects any invalidation range. Half-open [off, off+len). */
    void remove_entries_overlapping_data_ranges(int stripe_id, const std::vector<std::tuple<int, int, int>> &ranges_bid_off_len);

    uint64_t get_write_generation(int stripe_id);

  private:
    std::mutex mutex_;
    std::unordered_map<int, StripeState> stripes_;

    StripeState &ensure_stripe(int stripe_id);
    static bool apply_parity_delta_slice(Config *cfg, int parity_block_id, int data_block_id, const unsigned char *old_data,
                                         const unsigned char *new_data, unsigned char *parity_block, int ro, int len);
  };

} // namespace ECProject

#endif
