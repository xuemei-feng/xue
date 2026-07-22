#ifndef ECPROJECT_CORD_XUE_LRC_H
#define ECPROJECT_CORD_XUE_LRC_H

#include <algorithm>
#include <string>
#include <vector>

namespace ECProject
{
  namespace cord_xue_lrc
  {
    inline bool is_cord_xue_code(const std::string &code_type)
    {
      return code_type == "CordXueLRC";
    }

    /** Uniform 风格本地组：槽位尽量均分为 (k+r)/z 或 +1（较大组在末尾），最后一组含 r 个全局校验块。 */
    inline int local_group_slot_size(int k, int r, int z)
    {
      return (k + r) / z;
    }

    inline int larger_local_group_count(int k, int r, int z)
    {
      return (k + r) % z;
    }

    inline int local_group_slot_size_for_group(int g, int k, int r, int z)
    {
      const int base = local_group_slot_size(k, r, z);
      const int larger = larger_local_group_count(k, r, z);
      if (g + larger >= z)
        return base + 1;
      return base;
    }

    /** 第 g 个本地组内的数据块数量（最后一组需扣除 r 个全局槽位）。 */
    inline int data_block_count_in_local_group(int g, int k, int r, int z)
    {
      const int slots = local_group_slot_size_for_group(g, k, r, z);
      if (g == z - 1)
        return std::max(0, slots - r);
      return slots;
    }

    inline int max_data_blocks_in_any_local_group(int k, int r, int z)
    {
      int mx = 0;
      for (int g = 0; g < z; ++g)
        mx = std::max(mx, data_block_count_in_local_group(g, k, r, z));
      return mx;
    }

    /** 数据块 i 的逻辑本地组（与 UniformLRC 分组一致）。 */
    inline int data_block_map2group(int data_block_id, int k, int r, int z)
    {
      if (data_block_id < 0 || data_block_id >= k)
        return -1;
      int cursor = 0;
      for (int g = 0; g < z; ++g)
      {
        const int cnt = data_block_count_in_local_group(g, k, r, z);
        if (data_block_id < cursor + cnt)
          return g;
        cursor += cnt;
      }
      return z - 1;
    }

    inline int global_parity_map2group(int z)
    {
      return z - 1;
    }

    inline void build_data_blocks_per_local_group(int k, int r, int z, std::vector<std::vector<int>> *out)
    {
      out->assign(static_cast<size_t>(z), {});
      int cursor = 0;
      for (int g = 0; g < z; ++g)
      {
        const int cnt = data_block_count_in_local_group(g, k, r, z);
        for (int j = 0; j < cnt && cursor < k; ++j)
        {
          (*out)[static_cast<size_t>(g)].push_back(cursor++);
        }
      }
    }
  } // namespace cord_xue_lrc
} // namespace ECProject

#endif
