#include "parix_journal.h"
#include "unilrc_encoder.h"
#include <algorithm>
#include <cstring>
#include <iostream>

namespace ECProject
{
  namespace
  {
    bool interval_overlap_half_open(int a0, int alen, int b0, int blen)
    {
      const long long a1 = static_cast<long long>(a0) + alen;
      const long long b1 = static_cast<long long>(b0) + blen;
      return std::max(static_cast<long long>(a0), static_cast<long long>(b0)) < std::min(a1, b1);
    }

    bool entry_overlaps_any(const ParixJournal::Entry &e,
                            const std::vector<std::tuple<int, int, int>> &ranges_bid_off_len)
    {
      for (const auto &t : ranges_bid_off_len)
      {
        const int bid = std::get<0>(t);
        const int off = std::get<1>(t);
        const int len = std::get<2>(t);
        if (bid != e.data_block_id || len <= 0 || e.len <= 0)
        {
          continue;
        }
        if (interval_overlap_half_open(e.range_offset, e.len, off, len))
        {
          return true;
        }
      }
      return false;
    }

    bool slice_key_overlaps_any(int data_block_id, int range_offset, int len,
                                const std::vector<std::tuple<int, int, int>> &ranges_bid_off_len)
    {
      for (const auto &t : ranges_bid_off_len)
      {
        const int bid = std::get<0>(t);
        const int off = std::get<1>(t);
        const int invlen = std::get<2>(t);
        if (bid != data_block_id || invlen <= 0 || len <= 0)
        {
          continue;
        }
        if (interval_overlap_half_open(range_offset, len, off, invlen))
        {
          return true;
        }
      }
      return false;
    }
  } // namespace


  ParixJournal::StripeState &ParixJournal::ensure_stripe(int stripe_id)
  {
    return stripes_[stripe_id];
  }

  ParixJournal::AppendResult ParixJournal::append(int stripe_id, uint64_t batch_id, uint64_t write_generation,
                                                  int parity_block_id, const std::string &parity_block_key,
                                                  const std::string &parity_datanode_ip, int parity_datanode_port,
                                                  int data_block_id, int range_offset, int len,
                                                  const std::string &new_payload)
  {
    if (len <= 0 || static_cast<int>(new_payload.size()) != len)
    {
      return AppendResult::NEED_D0;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    StripeState &st = stripes_[stripe_id];
    if (write_generation != st.write_generation)
    {
      return AppendResult::NEED_D0;
    }
    const auto key = std::make_tuple(data_block_id, range_offset, len);
    Entry e;
    e.batch_id = batch_id;
    e.write_generation = write_generation;
    e.parity_block_id = parity_block_id;
    e.parity_block_key = parity_block_key;
    e.parity_datanode_ip = parity_datanode_ip;
    e.parity_datanode_port = parity_datanode_port;
    e.data_block_id = data_block_id;
    e.range_offset = range_offset;
    e.len = len;
    e.new_payload = new_payload;
    auto it = st.last_committed_dr.find(key);
    if (it != st.last_committed_dr.end() && static_cast<int>(it->second.size()) == len)
    {
      e.old_payload = it->second;
      e.has_old = true;
      st.entries.push_back(std::move(e));
      return AppendResult::SUCCESS;
    }
    st.entries.push_back(std::move(e));
    return AppendResult::NEED_D0;
  }

  bool ParixJournal::supply_d0(int stripe_id, uint64_t batch_id, uint64_t write_generation, int parity_block_id,
                               int data_block_id, int range_offset, int len, const std::string &old_payload)
  {
    if (static_cast<int>(old_payload.size()) != len)
    {
      return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    StripeState &st = stripes_[stripe_id];
    if (write_generation != st.write_generation)
    {
      return false;
    }
    for (Entry &e : st.entries)
    {
      if (e.replayed)
        continue;
      if (e.batch_id == batch_id && e.write_generation == write_generation && e.parity_block_id == parity_block_id &&
          e.data_block_id == data_block_id && e.range_offset == range_offset && e.len == len && !e.has_old)
      {
        e.old_payload = old_payload;
        e.has_old = true;
        return true;
      }
    }
    return false;
  }

  bool ParixJournal::apply_parity_delta_slice(Config *cfg, int parity_block_id, int data_block_id,
                                              const unsigned char *old_data, const unsigned char *new_data,
                                              unsigned char *parity_block, int ro, int len)
  {
    const int k = cfg->k;
    const int r = cfg->r;
    const int z = cfg->z;
    if (parity_block_id < k || parity_block_id >= k + r + z)
    {
      return false;
    }
    const size_t rows = static_cast<size_t>(k + r + z);
    std::vector<unsigned char> mat(rows * static_cast<size_t>(k));
    if (cfg->CodeType == "AzureLRC" || cfg->CodeType == "RandomLRC")
    {
      gen_azure_lrc_matrix(mat.data(), k, r, z);
    }
    else
    {
      gen_unilrc_matrix(mat.data(), k, r, z);
    }
    int row = 0;
    if (parity_block_id < k + r)
    {
      row = parity_block_id - k;
    }
    else
    {
      row = parity_block_id;
    }
    for (int i = 0; i < len; ++i)
    {
      const unsigned char delta = static_cast<unsigned char>(old_data[i] ^ new_data[i]);
      const unsigned char coef = mat[static_cast<size_t>(row) * static_cast<size_t>(k) + static_cast<size_t>(data_block_id)];
      if (coef == 0)
      {
        continue;
      }
      parity_block[ro + i] = static_cast<unsigned char>(parity_block[ro + i] ^ gf_mul(coef, delta));
    }
    return true;
  }

  bool ParixJournal::replay_batch(int stripe_id, uint64_t batch_id, Config *cfg,
                                  const std::function<bool(const std::string &key, int block_id, char *buf, size_t bs,
                                                           const char *ip, int port)> &read_parity,
                                  const std::function<bool(const std::string &key, int block_id, const char *buf,
                                                           size_t bs, const char *ip, int port)> &write_parity)
  {
    const size_t bs = cfg->BlockSize;
    std::lock_guard<std::mutex> lock(mutex_);
    StripeState &st = stripes_[stripe_id];

    std::vector<Entry *> pending;
    for (Entry &e : st.entries)
    {
      if (!e.replayed && e.batch_id == batch_id && e.has_old && e.write_generation == st.write_generation)
      {
        pending.push_back(&e);
      }
    }
    if (pending.empty())
    {
      return true;
    }

    std::map<int, std::vector<unsigned char>> parity_bufs;
    std::map<int, std::string> parity_keys;
    std::map<int, std::pair<std::string, int>> parity_nodes;

    for (Entry *e : pending)
    {
      const int pid = e->parity_block_id;
      if (parity_bufs.find(pid) == parity_bufs.end())
      {
        parity_bufs[pid].resize(bs);
        if (!read_parity(e->parity_block_key, pid, reinterpret_cast<char *>(parity_bufs[pid].data()), bs,
                         e->parity_datanode_ip.c_str(), e->parity_datanode_port))
        {
          std::cerr << "[ParixJournal] read parity block " << pid << " failed\n";
          return false;
        }
        parity_keys[pid] = e->parity_block_key;
        parity_nodes[pid] = {e->parity_datanode_ip, e->parity_datanode_port};
      }
      unsigned char *pbuf = parity_bufs[pid].data();
      if (!apply_parity_delta_slice(cfg, pid, e->data_block_id, reinterpret_cast<const unsigned char *>(e->old_payload.data()),
                                    reinterpret_cast<const unsigned char *>(e->new_payload.data()), pbuf, e->range_offset, e->len))
      {
        return false;
      }
    }

    for (auto &kv : parity_bufs)
    {
      const int pid = kv.first;
      const std::string &pk = parity_keys[pid];
      const std::string &ip = parity_nodes[pid].first;
      int port = parity_nodes[pid].second;
      if (!write_parity(pk, pid, reinterpret_cast<const char *>(kv.second.data()), bs, ip.c_str(), port))
      {
        std::cerr << "[ParixJournal] write parity block " << pid << " failed\n";
        return false;
      }
    }

    for (Entry *e : pending)
    {
      const auto key = std::make_tuple(e->data_block_id, e->range_offset, e->len);
      st.last_committed_dr[key] = e->new_payload;
      e->replayed = true;
    }

    std::vector<Entry> kept;
    kept.reserve(st.entries.size());
    for (Entry &e : st.entries)
    {
      if (!(e.replayed && e.batch_id == batch_id))
      {
        kept.push_back(std::move(e));
      }
    }
    st.entries = std::move(kept);
    return true;
  }

  void ParixJournal::remove_entries_overlapping_data_ranges(int stripe_id,
                                                            const std::vector<std::tuple<int, int, int>> &ranges_bid_off_len)
  {
    if (ranges_bid_off_len.empty())
    {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    StripeState &st = stripes_[stripe_id];
    std::vector<Entry> kept;
    kept.reserve(st.entries.size());
    for (Entry &e : st.entries)
    {
      if (!entry_overlaps_any(e, ranges_bid_off_len))
      {
        kept.push_back(std::move(e));
      }
    }
    st.entries = std::move(kept);

    for (auto it = st.last_committed_dr.begin(); it != st.last_committed_dr.end();)
    {
      const int bid = std::get<0>(it->first);
      const int ro = std::get<1>(it->first);
      const int ln = std::get<2>(it->first);
      if (slice_key_overlaps_any(bid, ro, ln, ranges_bid_off_len))
      {
        it = st.last_committed_dr.erase(it);
      }
      else
      {
        ++it;
      }
    }
  }

  uint64_t ParixJournal::get_write_generation(int stripe_id)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return stripes_[stripe_id].write_generation;
  }

} // namespace ECProject
