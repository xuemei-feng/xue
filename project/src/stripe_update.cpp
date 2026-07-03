#include "stripe_update.h"
#include "unilrc_encoder.h"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace ECProject
{
  namespace stripe_update
  {
    UpdateMode classify_update(const std::map<int, std::vector<std::pair<int, int>>> &block_intervals, int k,
                               int block_size)
    {
      if (is_full_stripe_update(block_intervals, k, block_size))
        return UpdateMode::FULL_STRIPE;
      return UpdateMode::PARTIAL;
    }

    bool is_full_stripe_update(const std::map<int, std::vector<std::pair<int, int>>> &block_intervals, int k,
                               int block_size)
    {
      if (static_cast<int>(block_intervals.size()) != k)
        return false;
      for (int bid = 0; bid < k; ++bid)
      {
        auto it = block_intervals.find(bid);
        if (it == block_intervals.end())
          return false;
        if (it->second.size() != 1)
          return false;
        if (it->second[0].first != 0 || it->second[0].second != block_size)
          return false;
      }
      return true;
    }

    int local_parity_block_id(int k, int r, int data_block_id)
    {
      (void)r;
      (void)data_block_id;
      return k + r;
    }

    std::vector<int> parity_targets_for_data_block(int k, int r, int data_block_id)
    {
      (void)data_block_id;
      std::vector<int> out;
      for (int g = k; g < k + r; ++g)
        out.push_back(g);
      return out;
    }

    void xor_range_into_block(char *block_data, int block_size, int offset, const char *delta, int len)
    {
      if (offset < 0 || len < 0 || offset + len > block_size)
        throw std::runtime_error("xor_range_into_block: range out of block bounds");
      for (int i = 0; i < len; ++i)
        block_data[offset + i] = static_cast<char>(block_data[offset + i] ^ delta[i]);
    }

    void compute_parity_deltas_from_data_deltas(const std::string &code_type, int k, int r, int z, int block_size,
                                                const std::map<int, std::vector<char>> &data_block_deltas,
                                                std::vector<std::vector<char>> *parity_deltas)
    {
      if (parity_deltas == nullptr)
        return;
      parity_deltas->assign(static_cast<size_t>(r + z), std::vector<char>(static_cast<size_t>(block_size), 0));
      if (data_block_deltas.empty())
        return;

      std::vector<int> block_ids;
      block_ids.reserve(data_block_deltas.size());
      for (const auto &kv : data_block_deltas)
        block_ids.push_back(kv.first);
      std::sort(block_ids.begin(), block_ids.end());

      const int data_block_num = static_cast<int>(block_ids.size());
      std::vector<unsigned char *> data_ptrs(static_cast<size_t>(data_block_num));
      for (int i = 0; i < data_block_num; ++i)
        data_ptrs[static_cast<size_t>(i)] = reinterpret_cast<unsigned char *>(
            const_cast<char *>(data_block_deltas.at(block_ids[static_cast<size_t>(i)]).data()));

      std::vector<unsigned char *> parity_ptrs(static_cast<size_t>(r + z));
      for (int i = 0; i < r + z; ++i)
        parity_ptrs[static_cast<size_t>(i)] =
            reinterpret_cast<unsigned char *>((*parity_deltas)[static_cast<size_t>(i)].data());

      unsigned char *encode_matrix = new unsigned char[static_cast<size_t>(k + r + z) * static_cast<size_t>(k)];
      if (code_type == "UniLRC")
        gen_unilrc_matrix(encode_matrix, k, r, z);
      else
        gen_azure_lrc_matrix(encode_matrix, k, r, z);

      unsigned char *sub_matrix = new unsigned char[static_cast<size_t>(r + z) * static_cast<size_t>(data_block_num)];
      for (int pi = 0; pi < r + z; ++pi)
      {
        for (int j = 0; j < data_block_num; ++j)
        {
          const int bid = block_ids[static_cast<size_t>(j)];
          sub_matrix[static_cast<size_t>(pi * data_block_num + j)] =
              encode_matrix[static_cast<size_t>((k + pi) * k + bid)];
        }
      }

      unsigned char *g_tbls = new unsigned char[static_cast<size_t>(data_block_num * (r + z) * 32)];
      ec_init_tables(data_block_num, r + z, sub_matrix, g_tbls);
      ec_encode_data_avx2(block_size, data_block_num, r + z, g_tbls, data_ptrs.data(), parity_ptrs.data());

      delete[] g_tbls;
      delete[] sub_matrix;
      delete[] encode_matrix;
    }
  } // namespace stripe_update
} // namespace ECProject
