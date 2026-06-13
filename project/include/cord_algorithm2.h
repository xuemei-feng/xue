#ifndef ECPROJECT_CORD_ALGORITHM2_H
#define ECPROJECT_CORD_ALGORITHM2_H

#include <cstdint>
#include <map>
#include <set>
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
      int slot_unit_bytes = 1;
      int center_global_block_id = -1; // 最后一组相交集中心（调试）
      std::set<int> local_parity_in_rack_data_blocks; // 可在机架内直接 XOR 更新本地校验的数据块 ID
    };

    /**
     * 算法二：输入算法一的分组 U、条带与块内更新区间。
     * |N|≥3 且算法三成功：先做 PDP+DCP，数据仅发往指派的全局校验收集器；各收集器再向其它全局块与局校验扇出（语义对齐原单中心星型的第二、三段）。
     * |N|=2 或算法三未成功：单全局中心星型（pop_c = argmin_c Σ_i t_{i,c}·b_i）。
     * |N|=1：MST(Kruskal) 于 V={d}∪{全局校验}。
     * 调度：链路按 slot_unit_bytes 切时隙；每时隙 Dinic 匹配（每 cluster 每时隙最多 1 发、1 收，可同时收发）。
     * 数据依赖：STAR_CENTER_TO_GLOBAL / STAR_CENTER_TO_LOCAL 仅在同 group_index 下、发往该收集器的
     * STAR_DATA_TO_CENTER 链路全部完成（该链路的 remaining 用尽）后才可参与匹配，保证 collector 收齐 Δ 并完成编码后再扇出校验增量。
     *
     * TrainLink.delta_kind：相交集链路上「发往收集器」段为数据增量 ΔD；收集器扇出为校验增量（由 proxy 对 Δ 聚合后再 XOR 落盘）。
     * |N|=1 的 MST 边均为数据增量在线上传输。
     */
    Algorithm2Result build_algorithm2(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<std::vector<int>> &U,
        int cluster_num,
        const TransferParams &tp,
        int slot_unit_bytes);

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
