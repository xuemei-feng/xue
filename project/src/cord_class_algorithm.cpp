#include "cord_class_algorithm.h"
#include "devcommon.h"
#include "meta_definition.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <map>
#include <set>
#include <utility>

namespace ECProject
{
  namespace cord_class
  {
    namespace
    {
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

      int local_parity_block_for_data(const Stripe &stripe, int data_bid)
      {
        if (data_bid < 0 || data_bid >= stripe.k)
          return -1;
        return local_parity_block_for_group(stripe, stripe.blocks[data_bid]->map2group);
      }

      int global_cluster_id(const Stripe &stripe)
      {
        if (stripe.k >= static_cast<int>(stripe.blocks.size()))
          return -1;
        return stripe.blocks[stripe.k]->map2cluster;
      }

      int collector_global_block(const Stripe &stripe) { return stripe.k; }

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

      double bw_mbps(int src, int dst, int cluster_num, const cord_alg2::TransferParams &tp)
      {
        if (src >= 0 && dst >= 0 && src < cluster_num && dst < cluster_num)
        {
          const double m = tp.bw_matrix_mb_per_sec[src][dst];
          if (m > 0.0)
            return m;
        }
        if (src == dst)
          return 1000.0;
        return 100.0;
      }

      double transfer_sec_matrix(int src_c, int dst_c, int64_t bytes, int cluster_num,
                                 const cord_alg2::TransferParams &tp)
      {
        if (bytes <= 0)
          return 0.0;
        const double lat =
            (src_c == dst_c) ? tp.same_cluster_latency_sec : tp.cross_cluster_latency_sec;
        const double mbps = bw_mbps(src_c, dst_c, cluster_num, tp);
        return lat + static_cast<double>(bytes) / (mbps * 1024.0 * 1024.0);
      }

      cord_alg2::TrainLink make_link(int src_b, int dst_b, int src_c, int dst_c, int64_t payload,
                                     cord_alg2::TrainLinkKind kind, cord_alg2::CordDeltaPayloadKind dk,
                                     int group_index, int cluster_num, const cord_alg2::TransferParams &tp,
                                     int mst_origin = -1)
      {
        cord_alg2::TrainLink L;
        L.src_block_id = src_b;
        L.dst_block_id = dst_b;
        L.src_cluster = src_c;
        L.dst_cluster = dst_c;
        L.payload_bytes = payload;
        L.est_transfer_sec = transfer_sec_matrix(src_c, dst_c, payload, cluster_num, tp);
        L.group_index = group_index;
        L.kind = kind;
        L.delta_kind = dk;
        L.mst_origin_data_block = mst_origin;
        return L;
      }

      void push_parity_write(CordIngressClusterHints *hints, const CordIngressParityWriteRec &rec, bool global)
      {
        if (hints == nullptr)
          return;
        if (global)
          hints->global_parity_writes.push_back(rec);
        else
          hints->local_parity_writes.push_back(rec);
      }

      void add_global_fanout(cord_alg2::Algorithm2Result *out, const Stripe &stripe,
                             const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
                             int group_index, int collector, int gc,
                             const std::vector<int> &merge_blocks, int cluster_num,
                             const cord_alg2::TransferParams &tp)
      {
        const int k = stripe.k;
        const int r = stripe.r;
        const int64_t parity_global_b =
            cord_alg2::merged_delta_hull_span_bytes(block_intervals, merge_blocks);
        if (parity_global_b <= 0)
          return;
        for (int g = k; g < k + r; ++g)
        {
          if (g == collector)
            continue;
          auto L = make_link(collector, g, gc, block_cluster(stripe, g), parity_global_b,
                             cord_alg2::TrainLinkKind::STAR_CENTER_TO_GLOBAL,
                             cord_alg2::CordDeltaPayloadKind::PARITY_DELTA, group_index, cluster_num, tp);
          L.parity_merge_data_block_ids = merge_blocks;
          out->train_route.push_back(std::move(L));
        }
      }

      void add_center_to_local(cord_alg2::Algorithm2Result *out, const Stripe &stripe,
                               const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
                               int group_index, int collector, int gc, int stripe_group,
                               const std::vector<int> &merge_blocks, int cluster_num,
                               const cord_alg2::TransferParams &tp)
      {
        const int Lb = local_parity_block_for_group(stripe, stripe_group);
        if (Lb < 0)
          return;
        const int64_t pl =
            cord_alg2::merged_delta_hull_span_bytes(block_intervals, merge_blocks);
        if (pl <= 0)
          return;
        const int lc = block_cluster(stripe, Lb);
        auto L = make_link(collector, Lb, gc, lc, pl, cord_alg2::TrainLinkKind::STAR_CENTER_TO_LOCAL,
                           cord_alg2::CordDeltaPayloadKind::PARITY_DELTA, group_index, cluster_num, tp);
        L.parity_merge_data_block_ids = merge_blocks;
        out->train_route.push_back(std::move(L));
      }

      // 选择中继 cluster：以完整传输时间为判据。中继两跳 dc→relay→gc 在执行层串行
      // （先收满 relay 再转发，见 MST_FORWARD 依赖），故总耗时 = transfer_sec(dc,relay) + transfer_sec(relay,gc)，
      // 两跳的 latency 与 payload 均计入；仅当严格快于直连 transfer_sec(dc,gc) 时才返回中继，否则返回 -1（直连）。
      int pick_relay_cluster(int dc, int gc, int64_t payload_bytes, int cluster_num,
                             const cord_alg2::TransferParams &tp)
      {
        const double direct = transfer_sec_matrix(dc, gc, payload_bytes, cluster_num, tp);
        int best = -1;
        double best_cost = direct;
        for (int c = 0; c < cluster_num; ++c)
        {
          if (c == dc || c == gc)
            continue;
          const double hop1 = transfer_sec_matrix(dc, c, payload_bytes, cluster_num, tp);
          const double hop2 = transfer_sec_matrix(c, gc, payload_bytes, cluster_num, tp);
          const double relay_cost = hop1 + hop2;
          if (relay_cost < best_cost)
          {
            best_cost = relay_cost;
            best = c;
          }
        }
        return best;
      }

    } // namespace

    CordDataBlockClass classify_data_block(const Stripe &stripe, int data_block_id)
    {
      if (data_block_id < 0 || data_block_id >= stripe.k)
        return CordDataBlockClass::CLASS3;
      const int dc = block_cluster(stripe, data_block_id);
      const int gc = global_cluster_id(stripe);
      const int lb = local_parity_block_for_data(stripe, data_block_id);
      if (lb >= 0 && dc == block_cluster(stripe, lb))
        return CordDataBlockClass::CLASS1;
      if (dc == gc)
        return CordDataBlockClass::CLASS2;
      return CordDataBlockClass::CLASS3;
    }

    cord_alg2::Algorithm2Result build_class_update_plan(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int cluster_num,
        const cord_alg2::TransferParams &tp,
        std::map<int, CordIngressClusterHints> *ingress_hints_by_cluster)
    {
      cord_alg2::Algorithm2Result out;
      const int k = stripe.k;
      const int gc = global_cluster_id(stripe);
      const int collector = collector_global_block(stripe);
      if (gc < 0 || collector < 0)
        return out;

      out.center_global_block_id = collector;
      int next_group = 0;

      std::map<int, std::vector<int>> class1_by_stripe_group;
      std::vector<int> class2_blocks;
      std::map<std::pair<int, int>, std::vector<int>> class3_groups;

      for (const auto &kv : block_intervals)
      {
        const int bid = kv.first;
        if (bid < 0 || bid >= k)
          continue;
        if (delta_bytes_for_block(block_intervals, bid) <= 0)
          continue;
        const CordDataBlockClass cls = classify_data_block(stripe, bid);
        if (cls == CordDataBlockClass::CLASS1)
          class1_by_stripe_group[stripe.blocks[bid]->map2group].push_back(bid);
        else if (cls == CordDataBlockClass::CLASS2)
          class2_blocks.push_back(bid);
        else
        {
          const int dc = block_cluster(stripe, bid);
          class3_groups[{dc, stripe.blocks[bid]->map2group}].push_back(bid);
        }
      }

      for (auto &kv : class1_by_stripe_group)
      {
        const int stripe_group = kv.first;
        std::vector<int> &blocks = kv.second;
        std::sort(blocks.begin(), blocks.end());
        const int gi = next_group++;
        const std::vector<int> merge_blocks = blocks;

        for (int d : blocks)
        {
          const int dc = block_cluster(stripe, d);
          const int64_t b = delta_bytes_for_block(block_intervals, d);
          const int relay = pick_relay_cluster(dc, gc, b, cluster_num, tp);
          if (relay < 0)
          {
            out.train_route.push_back(make_link(d, collector, dc, gc, b,
                                                cord_alg2::TrainLinkKind::STAR_DATA_TO_CENTER,
                                                cord_alg2::CordDeltaPayloadKind::DATA_DELTA, gi, cluster_num, tp));
          }
          else
          {
            auto hop1 = make_link(d, k + stripe.r + stripe_group, dc, relay, b,
                                  cord_alg2::TrainLinkKind::MST_FORWARD,
                                  cord_alg2::CordDeltaPayloadKind::DATA_DELTA, gi, cluster_num, tp, d);
            out.train_route.push_back(std::move(hop1));
            auto hop2 = make_link(k + stripe.r + stripe_group, collector, relay, gc, b,
                                  cord_alg2::TrainLinkKind::MST_FORWARD,
                                  cord_alg2::CordDeltaPayloadKind::DATA_DELTA, gi, cluster_num, tp, d);
            out.train_route.push_back(std::move(hop2));
          }

          if (ingress_hints_by_cluster)
          {
            const int lb = local_parity_block_for_data(stripe, d);
            if (lb >= 0)
            {
              CordIngressParityWriteRec wr;
              wr.block_id = lb;
              wr.block_key = stripe.blocks[lb]->block_key;
              wr.stripe_group = stripe_group;
              push_parity_write(&(*ingress_hints_by_cluster)[dc], wr, false);
            }
          }
        }

        add_global_fanout(&out, stripe, block_intervals, gi, collector, gc, merge_blocks, cluster_num, tp);
      }

      for (int d2 : class2_blocks)
      {
        const int gi = next_group++;
        const int stripe_group = stripe.blocks[d2]->map2group;
        if (ingress_hints_by_cluster)
        {
          CordIngressClusterHints &gh = (*ingress_hints_by_cluster)[gc];
          for (int g = k; g < k + stripe.r; ++g)
          {
            CordIngressParityWriteRec wr;
            wr.block_id = g;
            wr.block_key = stripe.blocks[g]->block_key;
            wr.stripe_group = stripe_group;
            if (std::find_if(gh.global_parity_writes.begin(), gh.global_parity_writes.end(),
                             [&](const CordIngressParityWriteRec &x) { return x.block_id == g; }) ==
                gh.global_parity_writes.end())
              push_parity_write(&gh, wr, true);
          }
          if (std::find(gh.cache_lp_stripe_groups.begin(), gh.cache_lp_stripe_groups.end(), stripe_group) ==
              gh.cache_lp_stripe_groups.end())
            gh.cache_lp_stripe_groups.push_back(static_cast<int32_t>(stripe_group));
        }
        const std::vector<int> merge{d2};
        add_center_to_local(&out, stripe, block_intervals, gi, collector, gc, stripe_group, merge, cluster_num,
                            tp);
      }

      for (auto &kv : class3_groups)
      {
        const int dc = kv.first.first;
        const int stripe_group = kv.first.second;
        std::vector<int> &S = kv.second;
        std::sort(S.begin(), S.end());
        const int gi = next_group++;
        const int Lb = local_parity_block_for_group(stripe, stripe_group);
        if (Lb < 0)
          continue;
        const int lc = block_cluster(stripe, Lb);
        const int64_t total_bytes =
            cord_alg2::merged_delta_hull_span_bytes(block_intervals, S);

        const double t_dg = transfer_sec_matrix(dc, gc, total_bytes, cluster_num, tp);
        const double t_dl = transfer_sec_matrix(dc, lc, total_bytes, cluster_num, tp);
        const double t_gl = transfer_sec_matrix(gc, lc, total_bytes, cluster_num, tp);
        const double chain_cost = t_dg + t_gl;
        const double concurrent_cost = std::max(t_dg, t_dl);
        const bool use_chain = chain_cost <= concurrent_cost;

        if (cord_verbose_enabled())
          std::cout << "[CoRD-Class] class3 dc=" << dc << " group=" << stripe_group << " |S|=" << S.size()
                    << " chain=" << chain_cost << "s concurrent=" << concurrent_cost << "s pick="
                    << (use_chain ? "chain" : "concurrent") << '\n';

        for (int d : S)
        {
          const int64_t b = delta_bytes_for_block(block_intervals, d);
          out.train_route.push_back(make_link(d, collector, dc, gc, b,
                                              cord_alg2::TrainLinkKind::STAR_DATA_TO_CENTER,
                                              cord_alg2::CordDeltaPayloadKind::DATA_DELTA, gi, cluster_num, tp));
        }

        add_global_fanout(&out, stripe, block_intervals, gi, collector, gc, S, cluster_num, tp);

        if (use_chain)
        {
          add_center_to_local(&out, stripe, block_intervals, gi, collector, gc, stripe_group, S, cluster_num,
                              tp);
        }
        else
        {
          if (ingress_hints_by_cluster)
          {
            auto &hints = (*ingress_hints_by_cluster)[dc];
            if (std::find(hints.cache_lp_stripe_groups.begin(), hints.cache_lp_stripe_groups.end(),
                          stripe_group) == hints.cache_lp_stripe_groups.end())
              hints.cache_lp_stripe_groups.push_back(static_cast<int32_t>(stripe_group));
          }
          const int64_t pl = cord_alg2::merged_delta_hull_span_bytes(block_intervals, S);
          auto L = make_link(S.front(), Lb, dc, lc, pl, cord_alg2::TrainLinkKind::STAR_DATA_TO_LOCAL,
                             cord_alg2::CordDeltaPayloadKind::PARITY_DELTA, gi, cluster_num, tp);
          L.parity_merge_data_block_ids = S;
          out.train_route.push_back(std::move(L));
        }
      }

      cord_alg2::schedule_train_route_timeslots(&out, cluster_num, tp);

      if (cord_verbose_enabled())
        std::cout << "[CoRD-Class] train_route links=" << out.train_route.size()
                  << " slots=" << out.timeslot_schedule.size() << '\n';
      return out;
    }
  } // namespace cord_class
} // namespace ECProject
