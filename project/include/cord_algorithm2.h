#ifndef ECPROJECT_CORD_ALGORITHM2_H
#define ECPROJECT_CORD_ALGORITHM2_H

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ECProject
{
  struct Stripe;
}

namespace ECProject
{
  namespace cord_alg2
  {
    /** 传输时间模型：t = latency + bytes * inv_bw（同/跨 cluster） */
    struct TransferParams
    {
      double same_cluster_latency_sec = 1e-4;
      double cross_cluster_latency_sec = 2e-3;
      double inv_bw_sec_per_byte = 1.0 / (100.0 * 1024.0 * 1024.0); // ~100 MiB/s
      /** 是否在 Dinic 调度中强制「每 cluster 每步最多 1 发 + 1 收」（默认 true，保持原有约束） */
      bool enforce_one_send_one_recv_per_cluster = true;
    };

    enum class TrainLinkKind
    {
      STAR_DATA_TO_CENTER,
      STAR_CENTER_TO_GLOBAL,
      STAR_CENTER_TO_LOCAL,
      MST_FORWARD
    };

    /** 线上载荷语义：数据增量 ΔD（按字节传输） vs 已由收集器聚合得到的校验增量（再 XOR 落盘） */
    enum class CordDeltaPayloadKind
    {
      DATA_DELTA,
      PARITY_DELTA
    };

    struct TrainLink
    {
      int src_block_id = -1;
      int dst_block_id = -1;
      int src_cluster = -1;
      int dst_cluster = -1;
      int64_t payload_bytes = 0;
      double est_transfer_sec = 0.0;
      int group_index = -1;
      TrainLinkKind kind = TrainLinkKind::MST_FORWARD;
      CordDeltaPayloadKind delta_kind = CordDeltaPayloadKind::DATA_DELTA;
      /** |N|=1 MST：载荷语义对应的数据块 id（中继边 src 可能为校验块） */
      int mst_origin_data_block = -1;
      /** 非空：校验增量合并仅基于这些数据块（同一 collector 子集）；空则由 proxy 按 stripe_group 回落 */
      std::vector<int> parity_merge_data_block_ids;
    };

    struct TimeslotEntry
    {
      int timeslot = 0;
      std::vector<int> link_indices;
    };

    struct Algorithm2Result
    {
      std::vector<TrainLink> train_route;
      std::vector<TimeslotEntry> timeslot_schedule;
      /** 全局 collector：更新块最多的机架（cluster）中任选一更新 data block */
      int collector_block_id = -1;
      int center_global_block_id = -1; // 与 collector_block_id 相同（调试兼容）
    };

    /**
     * 算法二（rack-collector）：全局唯一 collector + 分机架本地校验。
     * - 先找更新 data block 数量最多的 cluster；再比较该 cluster 的更新块数 vs 全局校验块最多的
     *   cluster 上的 global 块数，取更大一侧：更新侧 collector 为最小更新块，global 侧为最小 global 块。
     * - 所有更新块 STAR_DATA_TO_CENTER 发往 collector（同块跳过网络）
     * - collector 聚合后 STAR_CENTER_TO_GLOBAL 扇出至其余 global parity
     * - 各 cluster 内同 local group 更新块合并，STAR_CENTER_TO_LOCAL 发往 local parity（与全局路径可并行）
     * 调度：STAR_CENTER_TO_GLOBAL 须等 collector ingress 收齐；rack-local STAR_CENTER_TO_LOCAL 无此依赖。
     */
    Algorithm2Result build_algorithm2(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<std::vector<int>> &U,
        int cluster_num,
        const TransferParams &tp);

    std::string train_link_kind_name(TrainLinkKind k);

    /**
     * 校验增量载荷跨度：在同一 in-block offset 轴上，取所有涉及区间的最小 lo 与最大 hi_excl，
     * 长度为 hi_excl - lo（连续 hull，中间空洞在载荷与 XOR 中补零语义）。
     */
    int64_t merged_delta_hull_span_bytes(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<int> &data_block_ids);
  } // namespace cord_alg2
} // namespace ECProject

#endif
