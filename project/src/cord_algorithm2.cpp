#include "cord_algorithm2.h"
#include "meta_definition.h"
#include <algorithm>
#include <iostream>
#include <limits>
#include <map>
#include <utility>

namespace ECProject
{
  namespace cord_alg2
  {
    int64_t merged_delta_hull_span_bytes(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<int> &data_block_ids)
    {
      int lo = std::numeric_limits<int>::max();
      int hi_excl = std::numeric_limits<int>::min();
      for (int bid : data_block_ids)
      {
        auto it = block_intervals.find(bid);
        if (it == block_intervals.end())
          continue;
        for (const auto &seg : it->second)
        {
          if (seg.second <= seg.first)
            continue;
          lo = std::min(lo, seg.first);
          hi_excl = std::max(hi_excl, seg.second);
        }
      }
      if (lo >= hi_excl)
        return 0;
      return static_cast<int64_t>(hi_excl - lo);
    }

    namespace
    {
      int64_t delta_bytes_for_block(
          const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
          int block_id)
      {
        auto it = block_intervals.find(block_id);
        if (it == block_intervals.end())
          return 0;
        int64_t s = 0;
        for (const auto &seg : it->second)
          s += static_cast<int64_t>(seg.second - seg.first);
        return s;
      }

      int block_cluster(const Stripe &stripe, int bid)
      {
        if (bid < 0 || bid >= static_cast<int>(stripe.blocks.size()))
          return -1;
        return stripe.blocks[bid]->map2cluster;
      }

      int local_parity_block_for_group(const Stripe &stripe, int gnum)
      {
        for (int i = stripe.k + stripe.r; i < stripe.n; ++i)
        {
          if (stripe.blocks[i]->map2group == gnum && stripe.blocks[i]->block_type == 'L')
            return i;
        }
        return -1;
      }

      double transfer_sec(int src_c, int dst_c, int64_t bytes, const TransferParams &tp)
      {
        if (bytes <= 0)
          return 0.0;
        double lat = (src_c == dst_c) ? tp.same_cluster_latency_sec : tp.cross_cluster_latency_sec;
        return lat + static_cast<double>(bytes) * tp.inv_bw_sec_per_byte;
      }

    } // namespace

    std::string train_link_kind_name(TrainLinkKind k)
    {
      switch (k)
      {
      case TrainLinkKind::STAR_DATA_TO_CENTER:
        return "STAR_DATA_TO_CENTER";
      case TrainLinkKind::STAR_CENTER_TO_GLOBAL:
        return "STAR_CENTER_TO_GLOBAL";
      case TrainLinkKind::STAR_CENTER_TO_LOCAL:
        return "STAR_CENTER_TO_LOCAL";
      default:
        return "MST_FORWARD";
      }
    }

    Algorithm2Result build_algorithm2(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<std::vector<int>> &U,
        int cluster_num,
        const TransferParams &tp)
    {
      (void)U;
      (void)cluster_num;
      Algorithm2Result out;
      const int k = stripe.k;
      const int r = stripe.r;
      if (cluster_num <= 0 || k <= 0 || r <= 0)
        return out;

      std::vector<int> D;
      D.reserve(block_intervals.size());
      for (const auto &kv : block_intervals)
      {
        if (kv.first < 0 || kv.first >= k)
          continue;
        if (delta_bytes_for_block(block_intervals, kv.first) > 0)
          D.push_back(kv.first);
      }
      std::sort(D.begin(), D.end());
      D.erase(std::unique(D.begin(), D.end()), D.end());
      if (D.empty())
        return out;

      std::cout << "[CoRD-Alg2] ===== build_algorithm2 (rack-collector) |D|=" << D.size() << " r=" << r
                << " k=" << k << " cluster_num=" << cluster_num << " =====\n";

      std::map<int, int> rack_update_cnt;
      for (int d : D)
      {
        const int c = block_cluster(stripe, d);
        if (c >= 0)
          rack_update_cnt[c]++;
      }
      int data_rack = -1;
      int n_data = -1;
      for (const auto &rc : rack_update_cnt)
      {
        if (rc.second > n_data || (rc.second == n_data && (data_rack < 0 || rc.first < data_rack)))
        {
          n_data = rc.second;
          data_rack = rc.first;
        }
      }
      if (data_rack < 0 || n_data <= 0)
        return out;

      std::map<int, int> rack_global_cnt;
      for (int gpar = k; gpar < k + r; ++gpar)
      {
        const int gc = block_cluster(stripe, gpar);
        if (gc >= 0)
          rack_global_cnt[gc]++;
      }
      int global_rack = -1;
      int n_global = 0;
      for (const auto &rg : rack_global_cnt)
      {
        if (rg.second > n_global || (rg.second == n_global && (global_rack < 0 || rg.first < global_rack)))
        {
          n_global = rg.second;
          global_rack = rg.first;
        }
      }

      int collector_blk = -1;
      int collector_rack = -1;
      bool collector_is_global = false;
      if (n_global > n_data)
      {
        collector_is_global = true;
        collector_rack = global_rack;
        for (int gpar = k; gpar < k + r; ++gpar)
        {
          if (block_cluster(stripe, gpar) == global_rack)
          {
            collector_blk = gpar;
            break;
          }
        }
      }
      else
      {
        collector_rack = data_rack;
        for (int d : D)
        {
          if (block_cluster(stripe, d) == data_rack)
          {
            collector_blk = d;
            break;
          }
        }
      }
      if (collector_blk < 0)
        return out;

      const int collector_cc = block_cluster(stripe, collector_blk);
      out.collector_block_id = collector_blk;
      out.center_global_block_id = collector_blk;
      const int gi = 0;

      std::cout << "[CoRD-Alg2] collector="
                << (collector_is_global ? "global_blk" : "data_blk") << collector_blk << " cluster=c"
                << collector_rack << " (data_rack=c" << data_rack << " n_data=" << n_data
                << " global_rack=c" << global_rack << " n_global=" << n_global << ")\n";
      std::cout << "[CoRD-Alg2] updated blocks:";
      for (int d : D)
        std::cout << " " << d << "(c" << block_cluster(stripe, d) << ")";
      std::cout << "\n";

      const int64_t block_b = tp.block_byte_size > 0 ? tp.block_byte_size : 0;
      if (block_b <= 0)
        return out;

      for (int d : D)
      {
        if (d == collector_blk)
          continue;
        if (delta_bytes_for_block(block_intervals, d) <= 0)
          continue;
        const int64_t b = block_b;
        int dc = block_cluster(stripe, d);
        TrainLink L;
        L.src_block_id = d;
        L.dst_block_id = collector_blk;
        L.src_cluster = dc;
        L.dst_cluster = collector_cc;
        L.payload_bytes = b;
        L.est_transfer_sec = transfer_sec(dc, collector_cc, b, tp);
        L.group_index = gi;
        L.kind = TrainLinkKind::STAR_DATA_TO_CENTER;
        L.delta_kind = CordDeltaPayloadKind::DATA_DELTA;
        std::cout << "[CoRD-Alg2]   DATA_TO_CENTER: blk" << d << "(c" << dc << ") --ΔD " << b
                  << "B --> collector_blk" << collector_blk << "(c" << collector_cc << ")\n";
        out.train_route.push_back(std::move(L));
      }

      bool has_global_fanout = false;
      for (int gpar = k; gpar < k + r; ++gpar)
      {
        if (gpar == collector_blk)
          continue;
        int gc = block_cluster(stripe, gpar);
        TrainLink L;
        L.src_block_id = collector_blk;
        L.dst_block_id = gpar;
        L.src_cluster = collector_cc;
        L.dst_cluster = gc;
        L.payload_bytes = block_b;
        L.est_transfer_sec = transfer_sec(collector_cc, gc, block_b, tp);
        L.group_index = gi;
        L.kind = TrainLinkKind::STAR_CENTER_TO_GLOBAL;
        L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
        L.parity_merge_data_block_ids = D;
        std::cout << "[CoRD-Alg2]   CTR_TO_GLOBAL: collector_blk" << collector_blk << "(c" << collector_cc
                  << ") --ΔP " << block_b << "B --> global_blk" << gpar << "(c" << gc << ")\n";
        out.train_route.push_back(std::move(L));
        has_global_fanout = true;
      }
      // collector 自身是全局块时虽无扇出边，仍会产生 ΔG，需终态 ΣΔG→L_{z-1}
      if (!has_global_fanout && collector_is_global && r > 0)
        has_global_fanout = true;

      std::map<std::pair<int, int>, std::vector<int>> rack_local_groups;
      for (int d : D)
      {
        const int rc = block_cluster(stripe, d);
        const int gnum = stripe.blocks[d]->map2group;
        rack_local_groups[{rc, gnum}].push_back(d);
      }
      for (auto &rlg : rack_local_groups)
      {
        const int rc = rlg.first.first;
        const int gnum = rlg.first.second;
        std::vector<int> &blocks = rlg.second;
        std::sort(blocks.begin(), blocks.end());
        const int Lb = local_parity_block_for_group(stripe, gnum);
        if (Lb < 0)
          continue;
        const int lc = block_cluster(stripe, Lb);
        const int rep = blocks.front();
        TrainLink L;
        L.src_block_id = rep;
        L.dst_block_id = Lb;
        L.src_cluster = rc;
        L.dst_cluster = lc;
        L.payload_bytes = block_b;
        L.est_transfer_sec = transfer_sec(rc, lc, block_b, tp);
        L.group_index = gi;
        L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
        L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
        L.parity_merge_data_block_ids = blocks;
        std::cout << "[CoRD-Alg2]   RACK_TO_LOCAL: rack=c" << rc << " rep_blk" << rep << " --ΔP " << block_b
                  << "B --> local_blk" << Lb << "(c" << lc << ") group=" << gnum
                  << " merge_src=" << blocks.size() << " blocks\n";
        out.train_route.push_back(std::move(L));
      }

      // Uniform 折入：全局扇出结束后，collector 发一份 ΣΔG=⊕ΔG 到 L_{z-1}（跨 Proxy）
      if (has_global_fanout)
      {
        const int last_local_gnum = stripe.z - 1;
        const int Lb = local_parity_block_for_group(stripe, last_local_gnum);
        if (Lb >= 0)
        {
          const int lc = block_cluster(stripe, Lb);
          TrainLink L;
          L.src_block_id = collector_blk;
          L.dst_block_id = Lb;
          L.src_cluster = collector_cc;
          L.dst_cluster = lc;
          L.payload_bytes = block_b;
          L.est_transfer_sec = transfer_sec(collector_cc, lc, block_b, tp);
          L.group_index = gi;
          L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
          L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
          // 空 merge：标记为 ΣΔG 终态（非机架数据 XOR）
          L.parity_merge_data_block_ids.clear();
          std::cout << "[CoRD-Alg2]   CTR_TO_LOCAL(ΣΔG): collector_blk" << collector_blk << "(c"
                    << collector_cc << ") --ΣΔG " << block_b << "B --> local_blk" << Lb << "(c" << lc
                    << ") L_{z-1} group=" << last_local_gnum << "\n";
          out.train_route.push_back(std::move(L));
        }
      }

      std::cout << "[CoRD-Alg2] train_route links=" << out.train_route.size()
                << " (no timeslot/max-flow scheduling)\n";
      return out;
    }
  } // namespace cord_alg2
} // namespace ECProject
