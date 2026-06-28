#ifndef XUE_SLICE_LAYOUT_H
#define XUE_SLICE_LAYOUT_H

#include <algorithm>
#include <map>
#include <utility>
#include <vector>

namespace ECProject
{
  struct XueBlockSlice
  {
    int block_id = -1;
    int offset = 0;
    int len = 0;
  };

  inline bool xue_block_slice_less(const XueBlockSlice &a, const XueBlockSlice &b)
  {
    if (a.block_id != b.block_id)
    {
      return a.block_id < b.block_id;
    }
    return a.offset < b.offset;
  }

  inline void xue_add_sparse_slice(std::map<int, std::vector<std::pair<int, int>>> *dst, int block_id,
                                   int len, int off)
  {
    if (dst == nullptr || len <= 0)
    {
      return;
    }
    auto &vec = (*dst)[block_id];
    vec.push_back(std::make_pair(len, off));
    std::sort(vec.begin(), vec.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });
    std::vector<std::pair<int, int>> merged;
    for (const auto &s : vec)
    {
      const int cur_l = s.second;
      const int cur_r = s.second + s.first - 1;
      if (merged.empty())
      {
        merged.push_back(s);
        continue;
      }
      const int prev_l = merged.back().second;
      const int prev_r = merged.back().second + merged.back().first - 1;
      if (cur_l <= prev_r + 1)
      {
        const int new_r = std::max(prev_r, cur_r);
        merged.back().second = prev_l;
        merged.back().first = new_r - prev_l + 1;
      }
      else
      {
        merged.push_back(s);
      }
    }
    vec.swap(merged);
  }

  inline void xue_add_logical_range_to_block_slices(
      int block_size, int logical_start, int logical_end_exclusive,
      std::map<int, std::vector<std::pair<int, int>>> *out_block_slices)
  {
    if (out_block_slices == nullptr || logical_end_exclusive <= logical_start)
    {
      return;
    }
    int pos = logical_start;
    while (pos < logical_end_exclusive)
    {
      const int block_id = pos / block_size;
      const int block_offset = pos % block_size;
      const int take = std::min(block_size - block_offset, logical_end_exclusive - pos);
      xue_add_sparse_slice(out_block_slices, block_id, take, block_offset);
      pos += take;
    }
  }

  inline std::map<int, std::vector<std::pair<int, int>>> xue_logical_ranges_to_block_slices(
      const std::vector<std::pair<int, int>> &logical_ranges, int block_size)
  {
    std::map<int, std::vector<std::pair<int, int>>> block_slices;
    for (const auto &r : logical_ranges)
    {
      xue_add_logical_range_to_block_slices(block_size, r.first, r.second, &block_slices);
    }
    return block_slices;
  }

  inline void xue_extend_parity_slices_in_block_map(
      std::map<int, std::vector<std::pair<int, int>>> *block_slices, int k, int n, int block_size,
      int unit_size)
  {
    if (block_slices == nullptr)
    {
      return;
    }
    std::map<int, std::vector<std::pair<int, int>>> data_only;
    for (const auto &kv : *block_slices)
    {
      if (kv.first >= 0 && kv.first < k)
      {
        data_only[kv.first] = kv.second;
      }
    }
    for (const auto &kv : data_only)
    {
      for (const auto &slice : kv.second)
      {
        const int block_off = slice.second;
        const int block_end = block_off + slice.first - 1;
        const int u0 = block_off / unit_size;
        const int u1 = block_end / unit_size;
        const int parity_off = u0 * unit_size;
        const int parity_end = std::min(block_size - 1, (u1 + 1) * unit_size - 1);
        const int parity_len = parity_end - parity_off + 1;
        for (int i = k; i < n; i++)
        {
          xue_add_sparse_slice(block_slices, i, parity_len, parity_off);
        }
      }
    }
  }

  inline std::vector<XueBlockSlice> xue_collect_data_slices_for_blocks(
      const std::map<int, std::vector<std::pair<int, int>>> &block_slices,
      const std::vector<int> &data_block_ids)
  {
    std::vector<XueBlockSlice> out;
    for (int bid : data_block_ids)
    {
      auto it = block_slices.find(bid);
      if (it == block_slices.end())
      {
        continue;
      }
      for (const auto &slice : it->second)
      {
        XueBlockSlice rec;
        rec.block_id = bid;
        rec.offset = slice.second;
        rec.len = slice.first;
        out.push_back(rec);
      }
    }
    std::sort(out.begin(), out.end(), xue_block_slice_less);
    return out;
  }
} // namespace ECProject

#endif
