#include "parity_log_store.h"
#include "stripe_update.h"
#include <fstream>
#include <iostream>

namespace ECProject
{
  bool ParityLogStore::IntervalKey::operator<(const IntervalKey &o) const
  {
    if (data_block_id != o.data_block_id)
      return data_block_id < o.data_block_id;
    if (range_offset != o.range_offset)
      return range_offset < o.range_offset;
    return range_length < o.range_length;
  }

  ParityLogStore &ParityLogStore::instance()
  {
    static ParityLogStore inst;
    return inst;
  }

  ParityLogStore::ParityLog &ParityLogStore::log_for(int stripe_id, int parity_block_id)
  {
    std::lock_guard<std::mutex> lk(map_mu_);
    return logs_[std::make_pair(stripe_id, parity_block_id)];
  }

  uint64_t ParityLogStore::entry_bytes(const LogEntry &e)
  {
    return e.d0.size() + e.new_data.size();
  }

  uint64_t ParityLogStore::complete_bytes_locked(const ParityLog &log)
  {
    uint64_t total = 0;
    for (const auto &kv : log.entries)
    {
      if (kv.second.has_d0)
        total += entry_bytes(kv.second);
    }
    return total;
  }

  void ParityLogStore::recompute_used_bytes_locked(ParityLog &log)
  {
    uint64_t total = 0;
    for (const auto &kv : log.entries)
      total += entry_bytes(kv.second);
    log.used_bytes = total;
  }

  ParityLogAppendResult ParityLogStore::append_new_data(int stripe_id, int parity_block_id,
                                                        const std::string &parity_block_key, int data_block_id,
                                                        int range_offset, int range_length, int k, int r, int z,
                                                        int block_size, const std::string &code_type,
                                                        const char *new_data, int new_data_len)
  {
    ParityLogAppendResult out;
    if (new_data == nullptr || new_data_len != range_length || range_length <= 0 || parity_block_key.empty())
      return out;

    ParityLog &log = log_for(stripe_id, parity_block_id);
    std::lock_guard<std::mutex> lk(log.mu);
    log.parity_block_key = parity_block_key;
    log.k = k;
    log.r = r;
    log.z = z;
    log.block_size = block_size;
    log.code_type = code_type;
    IntervalKey key{data_block_id, range_offset, range_length};
    auto it = log.entries.find(key);
    if (it == log.entries.end())
    {
      LogEntry entry;
      entry.seq = log.next_seq++;
      entry.new_data.assign(new_data, new_data + new_data_len);
      log.entries.emplace(key, std::move(entry));
      recompute_used_bytes_locked(log);
      out.ok = true;
      out.need_d0 = true;
      return out;
    }

    LogEntry &entry = it->second;
    entry.seq = log.next_seq++;
    entry.new_data.assign(new_data, new_data + new_data_len);
    recompute_used_bytes_locked(log);
    out.ok = true;
    out.need_d0 = !entry.has_d0;
    return out;
  }

  bool ParityLogStore::store_d0(int stripe_id, int parity_block_id, int data_block_id, int range_offset,
                                int range_length, const char *d0, int d0_len)
  {
    if (d0 == nullptr || d0_len != range_length || range_length <= 0)
      return false;
    ParityLog &log = log_for(stripe_id, parity_block_id);
    std::lock_guard<std::mutex> lk(log.mu);
    IntervalKey key{data_block_id, range_offset, range_length};
    auto it = log.entries.find(key);
    if (it == log.entries.end())
      return false;
    LogEntry &entry = it->second;
    entry.has_d0 = true;
    entry.d0.assign(d0, d0 + d0_len);
    recompute_used_bytes_locked(log);
    return true;
  }

  bool ParityLogStore::clear_stripe_logs(int stripe_id, int parity_begin, int parity_end)
  {
    std::lock_guard<std::mutex> lk(map_mu_);
    for (auto it = logs_.begin(); it != logs_.end();)
    {
      if (it->first.first == stripe_id && it->first.second >= parity_begin && it->first.second < parity_end)
        it = logs_.erase(it);
      else
        ++it;
    }
    return true;
  }

  bool ParityLogStore::merge_log_locked(ParityLog &log, int stripe_id, int parity_block_id,
                                        const std::string &parity_block_storage_path, int k, int r, int z, int block_size,
                                        const std::string &code_type)
  {
    if (log.entries.empty())
      return true;

    std::map<int, std::vector<char>> data_deltas;
    std::vector<IntervalKey> merged_keys;
    for (const auto &kv : log.entries)
    {
      const IntervalKey &key = kv.first;
      const LogEntry &entry = kv.second;
      if (!entry.has_d0 || entry.new_data.size() != static_cast<size_t>(key.range_length) ||
          entry.d0.size() != static_cast<size_t>(key.range_length))
        continue;
      std::vector<char> delta(static_cast<size_t>(key.range_length));
      for (int i = 0; i < key.range_length; ++i)
        delta[static_cast<size_t>(i)] =
            static_cast<char>(entry.new_data[static_cast<size_t>(i)] ^ entry.d0[static_cast<size_t>(i)]);

      auto &acc = data_deltas[key.data_block_id];
      if (acc.empty())
        acc.assign(static_cast<size_t>(block_size), 0);
      stripe_update::xor_range_into_block(acc.data(), block_size, key.range_offset, delta.data(), key.range_length);
      merged_keys.push_back(key);
    }

    if (merged_keys.empty())
      return true;

    std::vector<std::vector<char>> parity_deltas;
    stripe_update::compute_parity_deltas_from_data_deltas(code_type, k, r, z, block_size, data_deltas, &parity_deltas);

    const int parity_idx = parity_block_id - k;
    if (parity_idx < 0 || parity_idx >= r + z)
      return false;

    std::string path = parity_block_storage_path;
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!f)
    {
      std::cout << "[ParityLog] merge open failed " << path << std::endl;
      return false;
    }
    std::vector<char> buf(static_cast<size_t>(block_size));
    f.read(buf.data(), block_size);
    if (!f)
    {
      std::cout << "[ParityLog] merge read failed " << path << std::endl;
      return false;
    }

    const auto &pd = parity_deltas[static_cast<size_t>(parity_idx)];
    for (int i = 0; i < block_size; ++i)
      buf[static_cast<size_t>(i)] = static_cast<char>(buf[static_cast<size_t>(i)] ^ pd[static_cast<size_t>(i)]);

    f.seekp(0);
    f.write(buf.data(), block_size);
    f.flush();
    if (!f)
    {
      std::cout << "[ParityLog] merge write failed " << path << std::endl;
      return false;
    }

    for (const IntervalKey &key : merged_keys)
      log.entries.erase(key);
    recompute_used_bytes_locked(log);
    std::cout << "[ParityLog] merged stripe=" << stripe_id << " parity_blk=" << parity_block_id << " path=" << path
              << " merged_entries=" << merged_keys.size() << " remaining_entries=" << log.entries.size() << std::endl;
    return true;
  }

  bool ParityLogStore::merge_if_full(int stripe_id, int parity_block_id, const std::string &parity_block_storage_path,
                                     int k, int r, int z, int block_size, const std::string &code_type)
  {
    ParityLog &log = log_for(stripe_id, parity_block_id);
    std::lock_guard<std::mutex> lk(log.mu);
    if (complete_bytes_locked(log) < static_cast<uint64_t>(stripe_update::PARITY_LOG_CAPACITY_BYTES))
      return true;
    return merge_log_locked(log, stripe_id, parity_block_id, parity_block_storage_path, k, r, z, block_size, code_type);
  }

  bool ParityLogStore::merge_if_full_cached(int stripe_id, int parity_block_id, int datanode_port)
  {
    ParityLog &log = log_for(stripe_id, parity_block_id);
    std::lock_guard<std::mutex> lk(log.mu);
    if (log.parity_block_key.empty() || log.block_size <= 0)
      return true;
    if (complete_bytes_locked(log) < static_cast<uint64_t>(stripe_update::PARITY_LOG_CAPACITY_BYTES))
      return true;
    const std::string storage_path =
        "./storage/" + std::to_string(datanode_port) + "/" + log.parity_block_key;
    return merge_log_locked(log, stripe_id, parity_block_id, storage_path, log.k, log.r, log.z, log.block_size,
                            log.code_type);
  }
} // namespace ECProject
