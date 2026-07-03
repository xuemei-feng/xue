#ifndef ECPROJECT_STRIPE_UPDATE_H
#define ECPROJECT_STRIPE_UPDATE_H

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ECProject
{
  namespace stripe_update
  {
    static constexpr int PARITY_LOG_CAPACITY_BYTES = 16 * 1024 * 1024;

    enum class UpdateMode
    {
      PARTIAL = 1,
      FULL_STRIPE = 2
    };

    /** 全条带：每个数据块 [0,block_size) 且覆盖全部 k 个块，逻辑区间无空洞。 */
    UpdateMode classify_update(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int k,
        int block_size);

    bool is_full_stripe_update(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int k,
        int block_size);

    int local_parity_block_id(int k, int r, int data_block_id);

    std::vector<int> parity_targets_for_data_block(int k, int r, int data_block_id);

    void compute_parity_deltas_from_data_deltas(
        const std::string &code_type,
        int k,
        int r,
        int z,
        int block_size,
        const std::map<int, std::vector<char>> &data_block_deltas,
        std::vector<std::vector<char>> *parity_deltas);

    void xor_range_into_block(char *block_data, int block_size, int offset, const char *delta, int len);
  } // namespace stripe_update
} // namespace ECProject

#endif
