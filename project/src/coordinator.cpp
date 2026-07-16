#include "coordinator.h"
#include "devcommon.h"
#include <cstdint>
#include "cord_algorithm2.h"
#include "cord_class_algorithm.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>
#include <chrono>
#include <limits>
#include <iostream>
#include <set>
#include <cmath>
#include <stdexcept>
#include <numeric>
#include <algorithm>
#include <thread>
#include <atomic>
#include <tuple>
#include <map>
#include <google/protobuf/repeated_field.h>

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

template <typename T>
inline std::vector<size_t> argsort(const std::vector<T> &v)
{
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);
  std::sort(idx.begin(), idx.end(), [&v](size_t i1, size_t i2)
            { return v[i1] < v[i2]; });
  return idx;
};

inline int rand_num(int range)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(0, range - 1);
  int num = dis(gen);
  return num;
};

namespace ECProject  //定义一个名为 ECProject 的命名空间，防止命名冲突
{
  namespace  // 匿名命名空间：里面的内容只在当前文件内可见（内部链接）
  {
    bool is_azure_like_code(const std::string &code_type) // 辅助函数：判断是否为 Azure 系列分组规则编码
    {
      return code_type == "AzureLRC" || code_type == "RandomLRC" || code_type == "SplitParityLRC" || code_type == "CordXueLRC";
    }

    int count_recovery_helper_groups(const std::vector<int> &recovery_group_ids, int dest_group_id)
    {
      int helper_num = 0;
      for (int gid : recovery_group_ids)
      {
        if (gid != dest_group_id)
          ++helper_num;
      }
      return helper_num;
    }

    // Partition surviving blocks of a local group by physical cluster (rack).
    // Used for CordXueLRC / Azure-like local repair when one group spans multiple racks:
    // each remote rack proxy XOR-partials its local helpers, then dest rack merges.
    std::map<int, std::vector<int>> partition_group_helpers_by_cluster(
        Stripe &t_stripe, const std::vector<int> &group_block_ids, int failed_block_id)
    {
      std::map<int, std::vector<int>> cluster_to_blocks;
      for (int bid : group_block_ids)
      {
        if (bid == failed_block_id)
          continue;
        Block *t_block = t_stripe.blocks[bid];
        cluster_to_blocks[t_block->map2cluster].push_back(bid);
      }
      return cluster_to_blocks;
    }

    // All data blocks 0..k-1 by physical cluster. Used for Azure-like / CordXueLRC
    // global-parity repair: each rack computes a GF-weighted partial via decode_azure_lrc;
    // XOR of rack partials equals the full global parity.
    std::map<int, std::vector<int>> partition_data_blocks_by_cluster(Stripe &t_stripe, int k)
    {
      std::map<int, std::vector<int>> cluster_to_blocks;
      for (int bid = 0; bid < k; ++bid)
      {
        Block *t_block = t_stripe.blocks[bid];
        cluster_to_blocks[t_block->map2cluster].push_back(bid);
      }
      return cluster_to_blocks;
    }

    // Legacy multi-group path only: dest global group has no data helpers.
    // Rack-partitioned global repair no longer uses this (dest reads local data blocks).
    bool skip_dest_local_helpers_for_azure_global(const std::string &code_type, int failed_block_id, int k, int r)
    {
      return is_azure_like_code(code_type) && failed_block_id >= k && failed_block_id < k + r;
    }

    /** Deterministic seed for placement RNG: same placement_seed + stripe_id -> same sequence (shuffle / cluster / node). */
    static void seed_placement_mt19937(std::mt19937 &gen, std::uint64_t placement_seed, int stripe_id)
    {
      const std::uint64_t mixed =
          placement_seed ^ (static_cast<std::uint64_t>(static_cast<std::uint32_t>(stripe_id)) * UINT64_C(0x9e3779b97f4a7c15));
      const std::uint32_t seeds[] = {
          static_cast<std::uint32_t>(mixed),
          static_cast<std::uint32_t>(mixed >> 32),
          static_cast<std::uint32_t>(stripe_id),
          static_cast<std::uint32_t>(stripe_id) ^ static_cast<std::uint32_t>(mixed >> 48)};
      std::seed_seq ss(seeds, seeds + sizeof(seeds) / sizeof(seeds[0]));
      gen.seed(ss);
    }

    // CoRD：半开区间 [a0,a1) 与 [b0,b1) 是否有非空交集
    bool cord_half_open_overlap(int a0, int a1, int b0, int b1)
    {
      return std::max(a0, b0) < std::min(a1, b1);
    }

    // 将条带逻辑地址 [logical_start, logical_end_exclusive) 映射为各数据块内半开区间并追加到 out
    void cord_add_logical_range_to_data_blocks(
        int block_size,
        int k,
        int logical_start,
        int logical_end_exclusive,
        std::map<int, std::vector<std::pair<int, int>>> *out_block_intervals)
    {
      int pos = logical_start;
      const int logical_last = logical_end_exclusive - 1;
      while (pos <= logical_last)
      {
        const int block_id = pos / block_size;
        if (block_id >= k)
          break;
        const int block_offset = pos % block_size;
        const int block_tail = block_size - block_offset;
        const int len = std::min(block_tail, logical_last - pos + 1);
        (*out_block_intervals)[block_id].push_back(std::make_pair(block_offset, block_offset + len));
        pos += len;
      }
    }

    bool cord_two_blocks_intersect(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int bid_a,
        int bid_b)
    {
      const auto &ia = block_intervals.at(bid_a);
      const auto &ib = block_intervals.at(bid_b);
      for (const auto &pa : ia)
        for (const auto &pb : ib)
          if (cord_half_open_overlap(pa.first, pa.second, pb.first, pb.second))
            return true;
      return false;
    }

    // CoRD 算法一：按「块内更新区间是否与其它块相交」做传递闭包分组
    std::vector<std::vector<int>> cord_partition_groups_algorithm1(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals)
    {
      std::vector<int> D;
      D.reserve(block_intervals.size());
      for (const auto &kv : block_intervals)
        D.push_back(kv.first);

      std::vector<std::vector<int>> U;
      while (!D.empty())
      {
        const int d_i = D.back();
        D.pop_back();
        std::vector<int> N;
        N.push_back(d_i);
        while (true)
        {
          int flag = 0;
          for (size_t j = 0; j < D.size(); ++j)
          {
            const int d_j = D[j];
            bool intersects_n = false;
            for (int bi : N)
            {
              if (cord_two_blocks_intersect(block_intervals, bi, d_j))
              {
                intersects_n = true;
                break;
              }
            }
            if (intersects_n)
            {
              N.push_back(d_j);
              D.erase(D.begin() + static_cast<std::ptrdiff_t>(j));
              flag = 1;
              break;
            }
          }
          if (!flag)
            break;
        }
        std::sort(N.begin(), N.end());
        U.push_back(std::move(N));
      }
      return U;
    }

    struct CordSliceRec
    {
      int block_id;
      int block_offset;
      int len;
    };

    int64_t cord_lp_delta_bytes(const std::map<int, std::vector<std::pair<int, int>>> &block_intervals, int block_id)
    {
      auto it = block_intervals.find(block_id);
      if (it == block_intervals.end())
        return 0;
      int64_t sum = 0;
      for (const auto &seg : it->second)
        sum += static_cast<int64_t>(seg.second - seg.first);
      return sum;
    }

    double cord_lp_transfer_sec(int src_c, int dst_c, int64_t bytes, const cord_alg2::TransferParams &tp)
    {
      if (bytes <= 0)
        return 0.0;
      double lat = (src_c == dst_c) ? tp.same_cluster_latency_sec : tp.cross_cluster_latency_sec;
      return lat + static_cast<double>(bytes) * tp.inv_bw_sec_per_byte;
    }

    int cord_lp_pick_hub_global(const Stripe &stripe,
                                const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
                                const std::vector<int> &data_blocks_in_group,
                                const cord_alg2::TransferParams &tp)
    {
      const int k = stripe.k;
      const int r = stripe.r;
      if (r <= 0)
        return k;
      int best_c = k;
      double best_cost = std::numeric_limits<double>::infinity();
      for (int cand = k; cand < k + r; ++cand)
      {
        int cc = stripe.blocks[cand]->map2cluster;
        double sum = 0.0;
        for (int d : data_blocks_in_group)
        {
          int64_t b = cord_lp_delta_bytes(block_intervals, d);
          if (b <= 0)
            continue;
          int dc = stripe.blocks[d]->map2cluster;
          sum += cord_lp_transfer_sec(dc, cc, b, tp);
        }
        if (sum < best_cost)
        {
          best_cost = sum;
          best_c = cand;
        }
      }
      return best_c;
    }

    int cord_lp_find_local_parity_block(const Stripe &stripe, int gnum)
    {
      for (int i = stripe.k + stripe.r; i < stripe.n; ++i)
      {
        if (stripe.blocks[i]->map2group == gnum && stripe.blocks[i]->block_type == 'L')
          return i;
      }
      return -1;
    }

    bool cord_lp_find_delta_blob_offset(const std::vector<CordSliceRec> &slices, int bid, int block_off, int seg_len,
                                        uint64_t *out_off)
    {
      uint64_t running = 0;
      for (const auto &sl : slices)
      {
        if (sl.block_id == bid && sl.block_offset == block_off && sl.len == seg_len)
        {
          *out_off = running;
          return true;
        }
        running += static_cast<uint64_t>(sl.len);
      }
      return false;
    }

    struct LpFetchSpec
    {
      std::string blob_key;
      std::string dn_ip;
      int dn_port = 0;
      uint64_t blob_off = 0;
      uint64_t read_len = 0;
      int acc_offset = 0;
      int data_block_id = -1;
      int source_cluster_id = -1;
    };

    struct LpWorkAgg
    {
      int local_block_id = -1;
      std::string local_block_key;
      std::string local_dn_ip;
      int local_dn_port = 0;
      int parity_slice_offset = 0;
      int parity_slice_size = 0;
      std::vector<LpFetchSpec> fetches;
    };

    bool run_cord_lp_global_hub_aggregation(
        const std::map<int, Cluster> &cluster_table,
        const std::map<std::string, std::unique_ptr<proxy_proto::proxyService::Stub>> &proxy_ptrs,
        int stripe_id,
        Stripe *stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        std::map<std::tuple<int, int, int>, LpWorkAgg> &agg)
    {
      const int k = stripe->k;
      cord_alg2::TransferParams tp;
      for (auto &kv : agg)
      {
        LpWorkAgg &w = kv.second;
        const int lg = stripe->blocks[w.local_block_id]->map2group;
        std::map<int, std::vector<std::pair<int, int>>> data_only_bi;
        for (const auto &bi : block_intervals)
        {
          if (bi.first >= 0 && bi.first < k)
            data_only_bi[bi.first] = bi.second;
        }
        if (data_only_bi.empty())
            continue;
        const auto components = cord_partition_groups_algorithm1(data_only_bi);
        size_t comp_idx = 0;
        for (const auto &N : components)
        {
          std::vector<int> N_data;
          for (int bid : N)
          {
            if (bid >= 0 && bid < k)
              N_data.push_back(bid);
          }
          if (N_data.empty())
                continue;
          bool touches_lg = false;
          for (int bid : N_data)
          {
            if (stripe->blocks[bid]->map2group == lg)
            {
              touches_lg = true;
            break;
          }
          }
          if (!touches_lg)
            continue;
          std::set<int> Nset(N.begin(), N.end());
          std::vector<LpFetchSpec> comp_fetches;
          for (const auto &f : w.fetches)
          {
            if (Nset.find(f.data_block_id) != Nset.end())
              comp_fetches.push_back(f);
          }
          if (comp_fetches.empty())
          continue;
          std::set<int> src_clusters;
          for (const auto &f : comp_fetches)
            src_clusters.insert(f.source_cluster_id);
          const int hub_blk = cord_lp_pick_hub_global(*stripe, block_intervals, N_data, tp);
          const int hub_c = stripe->blocks[hub_blk]->map2cluster;
          const int lp_c = stripe->blocks[w.local_block_id]->map2cluster;

          std::string sess = w.local_block_key + "_gh_" + std::to_string(stripe_id) + "_" +
                             std::to_string(w.parity_slice_offset) + "_" + std::to_string(w.parity_slice_size) +
                             "_hb" + std::to_string(hub_blk) + "_ci" + std::to_string(static_cast<unsigned long>(comp_idx));

          auto cit_hub = cluster_table.find(hub_c);
          auto cit_lp = cluster_table.find(lp_c);
          if (cit_hub == cluster_table.end() || cit_lp == cluster_table.end())
          {
            std::cout << "[CoRD-LP-GH] invalid cluster id hub=" << hub_c << " lp=" << lp_c << std::endl;
            return false;
          }
          std::string hub_proxy_key = cit_hub->second.proxy_ip + ":" + std::to_string(cit_hub->second.proxy_port);
          auto hub_stub_it = proxy_ptrs.find(hub_proxy_key);
          if (hub_stub_it == proxy_ptrs.end() || !hub_stub_it->second)
          {
            std::cout << "[CoRD-LP-GH] no stub for hub proxy " << hub_proxy_key << std::endl;
            return false;
          }

          grpc::ClientContext ctx_begin;
          proxy_proto::CordLpHubSessionBegin begin;
          begin.set_session_key(sess);
          begin.set_expected_partials(static_cast<int>(src_clusters.size()));
          begin.set_parity_slice_size(w.parity_slice_size);
          begin.set_parity_slice_offset(w.parity_slice_offset);
          begin.set_local_block_id(w.local_block_id);
          begin.set_local_block_key(w.local_block_key);
          begin.set_local_datanode_ip(w.local_dn_ip);
          begin.set_local_datanode_port(w.local_dn_port);
          begin.set_dest_lp_proxy_ip(cit_lp->second.proxy_ip);
          begin.set_dest_lp_proxy_port(cit_lp->second.proxy_port);
          begin.set_stripe_id(stripe_id);
          begin.set_hub_global_block_id(hub_blk);
          proxy_proto::SetReply rep_begin;
          grpc::Status stb = hub_stub_it->second->cordLpHubSessionBegin(&ctx_begin, begin, &rep_begin);
          if (!stb.ok() || !rep_begin.ifcommit())
          {
            std::cout << "[CoRD-LP-GH] session begin failed: " << stb.error_message() << std::endl;
            return false;
          }

          for (int sc : src_clusters)
          {
            auto cit_sc = cluster_table.find(sc);
            if (cit_sc == cluster_table.end())
            {
              std::cout << "[CoRD-LP-GH] invalid source cluster " << sc << std::endl;
              return false;
            }
            std::string sc_key = cit_sc->second.proxy_ip + ":" + std::to_string(cit_sc->second.proxy_port);
            auto sc_stub_it = proxy_ptrs.find(sc_key);
            if (sc_stub_it == proxy_ptrs.end() || !sc_stub_it->second)
            {
              std::cout << "[CoRD-LP-GH] no stub for source proxy " << sc_key << std::endl;
              return false;
            }
            proxy_proto::CordLpComputePartialAndPush cp;
            cp.set_session_key(sess);
            cp.set_hub_proxy_ip(cit_hub->second.proxy_ip);
            cp.set_hub_proxy_port(cit_hub->second.proxy_port);
            cp.set_parity_slice_size(w.parity_slice_size);
            bool set_blob = false;
            for (const auto &f : comp_fetches)
            {
              if (f.source_cluster_id != sc)
                continue;
              if (!set_blob)
              {
                cp.set_delta_blob_key(f.blob_key);
                cp.set_delta_datanode_ip(f.dn_ip);
                cp.set_delta_datanode_port(f.dn_port);
                set_blob = true;
              }
              auto *df = cp.add_fetches();
              df->set_blob_key(f.blob_key);
              df->set_datanode_ip(f.dn_ip);
              df->set_datanode_port(f.dn_port);
              df->set_blob_offset(f.blob_off);
              df->set_read_len(f.read_len);
              df->set_acc_offset(f.acc_offset);
            }
            grpc::ClientContext ctx_cp;
            proxy_proto::SetReply rep_cp;
            grpc::Status stc = sc_stub_it->second->cordLpComputePartialAndPush(&ctx_cp, cp, &rep_cp);
            if (!stc.ok() || !rep_cp.ifcommit())
            {
              std::cout << "[CoRD-LP-GH] compute/push from cluster " << sc << " failed: " << stc.error_message()
                        << std::endl;
              return false;
            }
          }
          comp_idx++;
        }
      }
      return true;
    }

    void fill_group_xor_hints_from_alg2(
        const cord_alg2::Algorithm2Result &alg2,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        proxy_proto::CordTransferPlan *plan)
    {
      plan->clear_group_xor_hints();
      std::map<int, std::set<int>> blocks_by_group;
      for (const auto &L : alg2.train_route)
      {
        if (L.delta_kind != cord_alg2::CordDeltaPayloadKind::DATA_DELTA)
          continue;
        int bid = -1;
        if (L.kind == cord_alg2::TrainLinkKind::STAR_DATA_TO_CENTER)
          bid = L.src_block_id;
        else if (L.kind == cord_alg2::TrainLinkKind::MST_FORWARD && L.mst_origin_data_block >= 0)
          bid = L.mst_origin_data_block;
        if (bid < 0)
          continue;
        blocks_by_group[L.group_index].insert(bid);
      }
      for (const auto &kv : blocks_by_group)
      {
        std::vector<int> ids(kv.second.begin(), kv.second.end());
        const int64_t m = cord_alg2::merged_delta_hull_span_bytes(block_intervals, ids);
        if (m <= 0)
          continue;
        proxy_proto::CordTransferGroupXorHint *h = plan->add_group_xor_hints();
        h->set_group_index(kv.first);
        h->set_xor_accum_byte_length(static_cast<uint64_t>(m));
      }
    }

    void enrich_cord_transfer_plan_block_stripe_groups(Stripe *stripe, proxy_proto::CordTransferPlan *plan)
    {
      plan->clear_cord_block_stripe_groups();
      if (stripe == nullptr || plan == nullptr)
        return;
      for (int bid = 0; bid < stripe->k; ++bid)
      {
        proxy_proto::CordBlockStripeGroup *g = plan->add_cord_block_stripe_groups();
        g->set_block_id(bid);
        g->set_stripe_group(stripe->blocks[bid]->map2group);
      }
    }

    void enrich_cord_transfer_plan_step_parity_filters(Stripe *stripe, proxy_proto::CordTransferPlan *plan)
    {
      if (stripe == nullptr || plan == nullptr)
        return;
      for (int i = 0; i < plan->steps_size(); ++i)
      {
        proxy_proto::CordTransferStep *st = plan->mutable_steps(i);
        st->clear_parity_ingest_stripe_group();
        if ((st->link_kind() != proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_LOCAL &&
             st->link_kind() != proxy_proto::CORD_TRANSFER_STAR_DATA_TO_LOCAL) ||
            st->delta_payload_kind() != proxy_proto::CORD_DELTA_PARITY)
          continue;
        const int dst = st->dst_block_id();
        if (dst < 0 || dst >= static_cast<int>(stripe->blocks.size()))
          continue;
        Block *bp = stripe->blocks[dst];
        if (bp->block_type != 'L')
          continue;
        st->set_parity_ingest_stripe_group(bp->map2group);
      }
    }

    void reorder_cord_plan_steps_execution(proxy_proto::CordTransferPlan *plan)
    {
      const int n = plan->steps_size();
      if (n <= 0)
        return;
      if (n == 1)
      {
        plan->mutable_steps(0)->set_step_index(0);
        return;
      }
      std::vector<int> order(static_cast<size_t>(n));
      std::iota(order.begin(), order.end(), 0);
      /** 按算法二 Dinic 时隙顺序执行，使 MST 与星型在满足 cluster 容量与收集器依赖的前提下穿插并行；
       * 收集器扇出晚于同组 STAR 数据到达已由 build_algorithm2 时间步调度保证。 */
      std::stable_sort(order.begin(), order.end(), [&](int a, int b) {
        const auto &sa = plan->steps(a);
        const auto &sb = plan->steps(b);
        if (sa.scheduled_slot() != sb.scheduled_slot())
          return sa.scheduled_slot() < sb.scheduled_slot();
        if (sa.src_proxy_cluster_id() != sb.src_proxy_cluster_id())
          return sa.src_proxy_cluster_id() < sb.src_proxy_cluster_id();
        if (sa.dst_proxy_cluster_id() != sb.dst_proxy_cluster_id())
          return sa.dst_proxy_cluster_id() < sb.dst_proxy_cluster_id();
        if (sa.link_kind() != sb.link_kind())
          return static_cast<int>(sa.link_kind()) < static_cast<int>(sb.link_kind());
        if (sa.delta_payload_kind() != sb.delta_payload_kind())
          return static_cast<int>(sa.delta_payload_kind()) < static_cast<int>(sb.delta_payload_kind());
        if (sa.group_index() != sb.group_index())
          return sa.group_index() < sb.group_index();
        if (sa.dst_block_id() != sb.dst_block_id())
          return sa.dst_block_id() < sb.dst_block_id();
        if (sa.src_block_id() != sb.src_block_id())
          return sa.src_block_id() < sb.src_block_id();
        if (sa.chunk_byte_offset() != sb.chunk_byte_offset())
          return sa.chunk_byte_offset() < sb.chunk_byte_offset();
        return a < b;
      });
      google::protobuf::RepeatedPtrField<proxy_proto::CordTransferStep> tmp;
      tmp.CopyFrom(plan->steps());
      plan->clear_steps();
      int new_idx = 0;
      for (int oi : order)
      {
        proxy_proto::CordTransferStep *st = plan->add_steps();
        st->CopyFrom(tmp.Get(oi));
        st->set_step_index(new_idx++);
      }
    }

    void enrich_cord_transfer_plan_topology(
        Stripe *stripe,
        const std::map<int, Cluster> &cluster_table,
        const std::map<int, Node> &node_table,
        ToolBox *toolbox,
        proxy_proto::CordTransferPlan *plan,
        const std::vector<std::pair<int, std::vector<CordSliceRec>>> &sorted_clusters)
    {
      plan->clear_cluster_endpoints();
      plan->clear_block_placements();
      plan->clear_delta_blob_refs();
      plan->clear_cluster_delta_layouts();
      plan->clear_ingress_cache_refs();
      for (const auto &cit : cluster_table)
      {
        proxy_proto::CordTransferClusterEndpoint *ep = plan->add_cluster_endpoints();
        ep->set_cluster_id(cit.first);
        ep->set_proxy_ip(cit.second.proxy_ip);
        ep->set_proxy_port(cit.second.proxy_port);
      }
      std::set<int> seen_blocks;
      auto add_block = [&](int bid) {
        if (bid < 0 || bid >= static_cast<int>(stripe->blocks.size()))
          return;
        if (!seen_blocks.insert(bid).second)
          return;
        Block *bp = stripe->blocks[bid];
        const Node &n = node_table.at(bp->map2node);
        proxy_proto::CordTransferBlockPlacement *p = plan->add_block_placements();
        p->set_block_id(bid);
        p->set_block_key(bp->block_key);
        p->set_datanode_ip(n.node_ip);
        p->set_datanode_port(n.node_port);
      };
      for (int i = 0; i < plan->steps_size(); ++i)
      {
        add_block(plan->steps(i).src_block_id());
        add_block(plan->steps(i).dst_block_id());
      }
      for (const auto &sc : sorted_clusters)
      {
        const int cid = sc.first;
        const auto &slices = sc.second;
        std::string cord_key = toolbox->gen_cord_key(plan->stripe_id(), cid);
        proxy_proto::CordTransferDeltaBlobRef *r = plan->add_delta_blob_refs();
        r->set_cluster_id(cid);
        r->set_cord_plan_key(cord_key);
        r->set_delta_blob_key(cord_key + "_delta");
        const Node &dn = node_table.at(cluster_table.at(cid).nodes.front());
        r->set_delta_datanode_ip(dn.node_ip);
        r->set_delta_datanode_port(dn.node_port);

        proxy_proto::CordTransferClusterDeltaLayout *lay = plan->add_cluster_delta_layouts();
        lay->set_cluster_id(cid);
        uint64_t run = 0;
        for (const auto &sl : slices)
        {
          lay->add_data_block_ids(sl.block_id);
          lay->add_delta_base_offset(run);
          lay->add_delta_total_length(static_cast<uint64_t>(sl.len));
          run += static_cast<uint64_t>(sl.len);
        }
      }
      for (const auto &sc : sorted_clusters)
      {
        proxy_proto::CordIngressCacheRef *cr = plan->add_ingress_cache_refs();
        cr->set_cluster_id(sc.first);
        cr->set_append_key(toolbox->gen_cord_key(plan->stripe_id(), sc.first));
      }
    }

    /** packed 顺序遍历块内区间时，将链路内 packed offset 映射为块内逻辑字节偏移（与 ingest memcpy 对齐）。 */
    static int64_t cord_packed_offset_to_logical_in_block(const std::vector<std::pair<int, int>> &segs,
                                                         int64_t packed_off)
    {
      if (packed_off < 0)
        return 0;
      int64_t remain = packed_off;
      for (const auto &seg : segs)
      {
        const int64_t len = static_cast<int64_t>(seg.second - seg.first);
        if (len <= 0)
            continue;
        if (remain < len)
          return static_cast<int64_t>(seg.first) + remain;
        remain -= len;
      }
      return static_cast<int64_t>(segs.empty() ? 0 : segs.back().second);
    }

    static void enrich_cord_transfer_plan_delta_segs(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals, int k_datablock,
        proxy_proto::CordTransferPlan *plan)
    {
      plan->clear_cord_block_delta_segs();
      for (const auto &kv : block_intervals)
      {
        const int bid = kv.first;
        if (bid < 0 || bid >= k_datablock)
          continue;
        for (const auto &seg : kv.second)
        {
          if (seg.second <= seg.first)
            continue;
          proxy_proto::CordBlockHalfOpenSeg *s = plan->add_cord_block_delta_segs();
          s->set_block_id(bid);
          s->set_lo(seg.first);
          s->set_hi_excl(seg.second);
        }
      }
    }

    /** STAR_DATA 一次传 packed ΔD：ingest 从首段 lo 起连续写入 sum(seg.len)；ready 判定须用 lo+packed 而非 max(hi)。 */
    static uint64_t cord_block_delta_ingress_buffer_end(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals, int block_id)
    {
      auto it = block_intervals.find(block_id);
      if (it == block_intervals.end() || it->second.empty())
        return 0;
      int64_t packed = 0;
      for (const auto &seg : it->second)
        packed += static_cast<int64_t>(seg.second - seg.first);
      const int64_t lo = cord_packed_offset_to_logical_in_block(it->second, 0);
      return static_cast<uint64_t>(lo + packed);
    }

    /** 将算法二的 train_route + timeslot_schedule 压平为 CordTransferPlan；每条链路一步传完 payload。 */
    proxy_proto::CordTransferPlan cord_transfer_plan_from_algorithm2(
        int stripe_id, const std::string &plan_key, const cord_alg2::Algorithm2Result &alg2, int k_datablock,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals)
    {
      proxy_proto::CordTransferPlan plan;
      plan.set_stripe_id(stripe_id);
      plan.set_plan_key(plan_key);
      plan.set_slot_unit_bytes(0);
      plan.set_k_datablock(k_datablock);
      const auto &sched = alg2.timeslot_schedule;
      plan.set_total_rounds(static_cast<uint32_t>(sched.size()));
      int step_idx = 0;
      for (const auto &ts : sched)
      {
        const uint32_t sched_step = static_cast<uint32_t>(ts.timeslot);
        for (int li : ts.link_indices)
        {
          if (li < 0 || li >= static_cast<int>(alg2.train_route.size()))
            continue;
          const cord_alg2::TrainLink &L = alg2.train_route[static_cast<size_t>(li)];
          const int64_t full = std::max<int64_t>(0, L.payload_bytes);
          if (full <= 0)
            continue;

          proxy_proto::CordTransferStep *st = plan.add_steps();
          st->set_step_index(step_idx++);
          st->set_src_proxy_cluster_id(L.src_cluster);
          st->set_dst_proxy_cluster_id(L.dst_cluster);
          st->set_src_block_id(L.src_block_id);
          st->set_dst_block_id(L.dst_block_id);
          st->set_payload_bytes(static_cast<uint64_t>(full));
          st->set_link_kind(static_cast<proxy_proto::CordTransferLinkKind>(static_cast<int>(L.kind)));
          st->set_scheduled_slot(sched_step);
          st->set_depends_on_step_index(-1);
          st->set_estimated_transfer_sec(L.est_transfer_sec);
          st->set_group_index(L.group_index);
          st->set_delta_payload_kind(L.delta_kind == cord_alg2::CordDeltaPayloadKind::PARITY_DELTA
                                         ? proxy_proto::CORD_DELTA_PARITY
                                         : proxy_proto::CORD_DELTA_DATA);
          uint64_t chunk_off = 0;
          if (L.kind == cord_alg2::TrainLinkKind::STAR_DATA_TO_CENTER &&
              L.delta_kind == cord_alg2::CordDeltaPayloadKind::DATA_DELTA)
          {
            auto bit = block_intervals.find(L.src_block_id);
            if (bit != block_intervals.end())
              chunk_off = static_cast<uint64_t>(
                  cord_packed_offset_to_logical_in_block(bit->second, 0));
          }
          else if (L.kind == cord_alg2::TrainLinkKind::MST_FORWARD &&
                   L.mst_origin_data_block >= 0)
          {
            auto bit = block_intervals.find(L.mst_origin_data_block);
            if (bit != block_intervals.end())
              chunk_off = static_cast<uint64_t>(
                  cord_packed_offset_to_logical_in_block(bit->second, 0));
          }
          st->set_chunk_byte_offset(chunk_off);
          st->set_chunk_byte_length(static_cast<uint64_t>(full));
          if (L.mst_origin_data_block >= 0)
            st->set_mst_origin_data_block_id(L.mst_origin_data_block);
          else
            st->clear_mst_origin_data_block_id();
          st->clear_parity_merge_data_block_ids();
          for (int pb : L.parity_merge_data_block_ids)
            st->add_parity_merge_data_block_ids(pb);
        }
      }
      return plan;
    }

  } // namespace

  grpc::Status CoordinatorImpl::setParameter(
      grpc::ServerContext *context,
      const coordinator_proto::Parameter *parameter,
      coordinator_proto::RepIfSetParaSuccess *setParameterReply)
  {
    ECSchema system_metadata(parameter->partial_decoding(),
                             (ECProject::EncodeType)parameter->encodetype(),
                             (ECProject::SingleStripePlacementType)parameter->s_stripe_placementtype(),
                             (ECProject::MultiStripesPlacementType)parameter->m_stripe_placementtype(),
                             parameter->k_datablock(),
                             parameter->l_localparityblock(),
                             parameter->g_m_globalparityblock(),
                             parameter->b_datapergroup(),
                             parameter->x_stripepermergegroup());
    m_encode_parameters = system_metadata;
    setParameterReply->set_ifsetparameter(true);
    m_cur_cluster_id = 0;
    m_cur_stripe_id = 0;
    m_object_commit_table.clear();
    m_object_updating_table.clear();
    m_stripe_deleting_table.clear();
    for (auto it = m_cluster_table.begin(); it != m_cluster_table.end(); it++)
    {
      Cluster &t_cluster = it->second;
      t_cluster.blocks.clear();
      t_cluster.stripes.clear();
    }
    for (auto it = m_node_table.begin(); it != m_node_table.end(); it++)
    {
      Node &t_node = it->second;
      t_node.stripes.clear();
    }
    m_stripe_table.clear();
    m_merge_groups.clear();
    m_free_clusters.clear();
    m_agg_start_cid = 0;
    std::cout << "setParameter success" << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::sayHelloToCoordinator(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string prefix("Hello ");
    helloReplyFromCoordinator->set_message(prefix + helloRequestToCoordinator->name());
    std::cout << prefix + helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadOriginKeyValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPPort *proxyIPPort)
  {

    std::string key = keyValueSize->key();
    m_mutex.lock();
    m_object_commit_table.erase(key);
    m_mutex.unlock();
    int valuesizebytes = keyValueSize->valuesizebytes();

    ObjectInfo new_object;

    int k = m_encode_parameters.k_datablock;
    int g_m = m_encode_parameters.g_m_globalparityblock;
    int l = m_encode_parameters.l_localparityblock;
    // int b = m_encode_parameters.b_datapergroup;
    new_object.object_size = valuesizebytes;
    int block_size = ceil(valuesizebytes, k);

    proxy_proto::ObjectAndPlacement object_placement;
    object_placement.set_key(key);
    object_placement.set_valuesizebyte(valuesizebytes);
    object_placement.set_k(k);
    object_placement.set_g_m(g_m);
    object_placement.set_l(l);
    object_placement.set_encode_type((int)m_encode_parameters.encodetype);
    object_placement.set_block_size(block_size);

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.k = k;
    t_stripe.l = l;
    t_stripe.g_m = g_m;
    t_stripe.object_keys.push_back(key);
    t_stripe.object_sizes.push_back(valuesizebytes);
    m_stripe_table[t_stripe.stripe_id] = t_stripe;
    new_object.map2stripe = t_stripe.stripe_id;

    int s_cluster_id = generate_placement(t_stripe.stripe_id, block_size);

    Stripe &stripe = m_stripe_table[t_stripe.stripe_id];
    object_placement.set_stripe_id(stripe.stripe_id);
    for (int i = 0; i < int(stripe.blocks.size()); i++)
    {
      object_placement.add_datanodeip(m_node_table[stripe.blocks[i]->map2node].node_ip);
      object_placement.add_datanodeport(m_node_table[stripe.blocks[i]->map2node].node_port);
      object_placement.add_blockkeys(stripe.blocks[i]->block_key);
    }

    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    std::string selected_proxy_ip = m_cluster_table[s_cluster_id].proxy_ip;
    int selected_proxy_port = m_cluster_table[s_cluster_id].proxy_port;
    std::string chosen_proxy = selected_proxy_ip + ":" + std::to_string(selected_proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->encodeAndSetObject(&cont, object_placement, &set_reply);
    proxyIPPort->set_proxyip(selected_proxy_ip);
    proxyIPPort->set_proxyport(selected_proxy_port + ECProject::SET_XFER_PORT_OFFSET); // use another port to accept data
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[key] = new_object;
      m_mutex.unlock();
    }
    else
    {
      std::cout << "[SET] Send object placement failed!" << std::endl;
    }

    return grpc::Status::OK;
  }

  void CoordinatorImpl::initialize_optimal_lrc_stripe_placement(Stripe *stripe)
  {
    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;
    int group_size = stripe->r + 1;
    int local_group_size = int(stripe->k / stripe->z);
    int group_num_of_one_local_group = local_group_size / group_size;
    if (local_group_size % group_size != 0)
      group_num_of_one_local_group++;

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = (i % local_group_size / group_size) + i / local_group_size * group_num_of_one_local_group;
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = stripe->z * group_num_of_one_local_group;
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = (i - stripe->k - stripe->r + 1) * group_num_of_one_local_group - 1;
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_uniform_lrc_stripe_placement(Stripe *stripe)
  {
    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;

    int group_size = stripe->r + 1;
    int local_group_size = int((stripe->k + stripe->r) / stripe->z);
    int larger_local_group_num = int((stripe->k + stripe->r) % stripe->z);
    int group_num = -1;
    int block_num = 0;

    for (int i = 0; i < stripe->z; i++)
    {
      if (i + larger_local_group_num == stripe->z)
      {
        local_group_size++;
      }
      for (int j = 0; j < local_group_size; j++)
      {
        if (j % group_size == 0)
        {
          group_num++;
        }
        blocks_info[block_num++].map2group = group_num;
      }
      blocks_info[stripe->k + stripe->r + i].map2group = group_num;
    }
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_unilrc_and_azurelrc_stripe_placement(Stripe *stripe)
  {
    std::string code_type = m_sys_config->CodeType;

    // range 0~k-1: data blocks
    // range k~k+r-1: global parity blocks
    // range k+r~k+r+z-1: local parity blocks
    Block *blocks_info = new Block[stripe->n];
    // a stripe is only created by a single client
    assert(stripe->object_keys.size() == 1);
    // choose a cluster: round robin
    int t_cluster_id = stripe->stripe_id % m_sys_config->ClusterNum;
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / (stripe->k / stripe->z));
      }
      else if (i >= stripe->k && i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        if (code_type == "UniLRC")
        {
          blocks_info[i].map2group = int((i - stripe->k) / (stripe->r / stripe->z));
        }
        else if (is_azure_like_code(code_type))
        {
          blocks_info[i].map2group = int(stripe->z);
        }
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = int((i - stripe->k - stripe->r) / (stripe->z / stripe->z));
      }
      blocks_info[i].map2cluster = (t_cluster_id + blocks_info[i].map2group) % m_sys_config->ClusterNum;
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_xue_tripe_placement(Stripe *stripe)
  {
    (void)stripe;
    throw std::runtime_error("XueLRC placement strategy has been removed");
  }

  void CoordinatorImpl::initialize_random_lrc_stripe_placement(Stripe *stripe)
  {
    // Random placement:
    // 1) 从条带全部块中随机顺序投放
    // 2) 仅在 4 个随机 cluster 中放置（若总 cluster < 4，则使用全部）
    // 3) 约束：同一 cluster 块数 <= r + l，l 为该 cluster 中“去除全局校验组后的跨 group 数”
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);

    const int cluster_num = m_sys_config->ClusterNum;
    if (cluster_num <= 0)
    {
      throw std::runtime_error("ClusterNum must be positive for RandomLRC placement");
    }

    const int target_cluster_num = std::min(4, cluster_num);
    std::vector<int> selected_clusters;
    selected_clusters.reserve(target_cluster_num);
    // 轮询选择紧邻 cluster：以 stripe_id 为起点，按环形连续取 4 个。
    const int start_cluster = stripe->stripe_id % cluster_num;
    for (int i = 0; i < target_cluster_num; ++i)
    {
      selected_clusters.push_back((start_cluster + i) % cluster_num);
    }

    std::mt19937 gen;
    const std::uint64_t placement_seed = m_sys_config->PlacementRandomSeed;
    if (placement_seed != 0ULL)
    {
      seed_placement_mt19937(gen, placement_seed, stripe->stripe_id);
    }
    else
    {
      std::random_device rd;
      gen.seed(rd());
    }

    // 按 Azure 风格构建 group：数据组 0..z-1，全局校验组 z，本地校验组 0..z-1。
    const int global_parity_group_id = stripe->z;
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / (stripe->k / stripe->z));
      }
      else if (i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = global_parity_group_id;
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - stripe->k - stripe->r;
      }
    }

    std::vector<int> block_order(stripe->n);
    std::iota(block_order.begin(), block_order.end(), 0);
    const int max_attempts = 256;
    bool placed = false;
    std::vector<int> assigned_cluster(stripe->n, -1);

    for (int attempt = 0; attempt < max_attempts && !placed; ++attempt)
    {
      std::shuffle(block_order.begin(), block_order.end(), gen);
      std::fill(assigned_cluster.begin(), assigned_cluster.end(), -1);
      std::map<int, int> cluster_block_count;
      std::map<int, std::set<int>> cluster_groups_excluding_global;
      bool ok = true;

      for (int block_idx : block_order)
      {
        std::vector<int> candidate_clusters = selected_clusters;
        std::shuffle(candidate_clusters.begin(), candidate_clusters.end(), gen);
        bool assigned = false;
        const int block_group = blocks_info[block_idx].map2group;

        for (int cid : candidate_clusters)
        {
          int next_block_count = cluster_block_count[cid] + 1;
          int next_group_count = static_cast<int>(cluster_groups_excluding_global[cid].size());
          if (block_group != global_parity_group_id &&
              cluster_groups_excluding_global[cid].find(block_group) == cluster_groups_excluding_global[cid].end())
          {
            next_group_count++;
          }
          if (next_block_count <= stripe->r + next_group_count)
          {
            assigned_cluster[block_idx] = cid;
            cluster_block_count[cid] = next_block_count;
            if (block_group != global_parity_group_id)
            {
              cluster_groups_excluding_global[cid].insert(block_group);
            }
            assigned = true;
            break;
          }
        }

        if (!assigned)
        {
          ok = false;
        break;
      }
    }
      placed = ok;
    }

    if (!placed)
    {
      throw std::runtime_error("RandomLRC placement failed to satisfy cluster constraints");
    }

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].map2cluster = assigned_cluster[i];
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id, gen);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::initialize_split_parity_lrc_stripe_placement(Stripe *stripe)
  {
    // 6-cluster 轮询放置（stripe_id % 6）：
    //   slot0 -> 全部全局校验块；slot1 -> 全部本地校验块；slot2..5 -> 仅数据块（随机，且每 cluster 数据块数 <= r+1）
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);

    const int cluster_num = m_sys_config->ClusterNum;
    if (cluster_num < 6)
    {
      throw std::runtime_error("ClusterNum must be >= 6 for SplitParityLRC placement");
    }
    if (stripe->k > 4 * (stripe->r + 1))
    {
      throw std::runtime_error("SplitParityLRC requires k <= 4*(r+1) (four data clusters, each holds at most r+1 data blocks)");
    }

    const int base = stripe->stripe_id % 6;
    auto slot_cluster = [&](int slot_offset) -> int {
      return (base + slot_offset) % cluster_num;
    };
    const int global_cluster = slot_cluster(0);
    const int local_cluster = slot_cluster(1);
    std::vector<int> data_clusters;
    data_clusters.reserve(4);
    for (int slot = 2; slot <= 5; ++slot)
    {
      data_clusters.push_back(slot_cluster(slot));
    }

    std::mt19937 gen;
    const std::uint64_t placement_seed = m_sys_config->PlacementRandomSeed;
    if (placement_seed != 0ULL)
    {
      seed_placement_mt19937(gen, placement_seed, stripe->stripe_id);
    }
    else
    {
      std::random_device rd;
      gen.seed(rd());
    }

    const int global_parity_group_id = stripe->z;
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / (stripe->k / stripe->z));
      }
      else if (i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = global_parity_group_id;
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - stripe->k - stripe->r;
      }
    }

    std::vector<int> assigned_cluster(stripe->n, -1);
    for (int i = stripe->k; i < stripe->k + stripe->r; ++i)
    {
      assigned_cluster[i] = global_cluster;
    }
    for (int i = stripe->k + stripe->r; i < stripe->n; ++i)
    {
      assigned_cluster[i] = local_cluster;
    }

    std::vector<int> data_order(stripe->k);
    std::iota(data_order.begin(), data_order.end(), 0);
    const int max_attempts = 256;
    bool placed = false;
    for (int attempt = 0; attempt < max_attempts && !placed; ++attempt)
    {
      std::shuffle(data_order.begin(), data_order.end(), gen);
      std::map<int, int> data_cluster_count;
      for (int cid : data_clusters)
      {
        data_cluster_count[cid] = 0;
      }
      bool ok = true;
      for (int block_idx : data_order)
      {
        std::vector<int> candidates = data_clusters;
        std::shuffle(candidates.begin(), candidates.end(), gen);
        bool assigned = false;
        for (int cid : candidates)
        {
          if (data_cluster_count[cid] + 1 <= stripe->r + 1)
          {
            assigned_cluster[block_idx] = cid;
            data_cluster_count[cid]++;
            assigned = true;
            break;
          }
        }
        if (!assigned)
        {
          ok = false;
          break;
        }
      }
      placed = ok;
    }
    if (!placed)
    {
      throw std::runtime_error("SplitParityLRC placement failed to satisfy per-cluster data block count <= r+1");
    }

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].map2cluster = assigned_cluster[i];
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id, gen);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }


  void CoordinatorImpl::initialize_cord_xue_lrc_stripe_placement(Stripe *stripe)
  {
    // Azure-style placement (cluster = rack):
    // 1) all global parity -> global_cluster
    // 2) per local group: local parity + min(r,h) data blocks -> dedicated cluster (round-robin, skip global)
    // 3) one data block per group -> global_cluster
    // 4) remaining data per group in batches of (r+1) -> round-robin clusters (skip global)
    // 5) equal per-group remainder m>0: pack theta groups' remainders per cluster (theta=floor(r/(m-1)), m=1 -> r+1)
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);

    const int cluster_num = m_sys_config->ClusterNum;
    if (cluster_num < 3)
    {
      throw std::runtime_error("ClusterNum must be >= 3 for CordXueLRC placement");
    }
    if (stripe->k % stripe->z != 0)
    {
      throw std::runtime_error("CordXueLRC requires k divisible by z");
    }

    const int h = stripe->k / stripe->z;
    const int global_cluster = stripe->stripe_id % cluster_num;
    int cluster_cursor = global_cluster + 1;

    auto next_non_global_cluster = [&]() -> int {
      int cid = cluster_cursor % cluster_num;
      cluster_cursor++;
      while (cid == global_cluster)
      {
        cid = cluster_cursor % cluster_num;
        cluster_cursor++;
      }
      return cid;
    };

    std::mt19937 gen;
    const std::uint64_t placement_seed = m_sys_config->PlacementRandomSeed;
    if (placement_seed != 0ULL)
    {
      seed_placement_mt19937(gen, placement_seed, stripe->stripe_id);
    }
    else
    {
      std::random_device rd;
      gen.seed(rd());
    }

    const int global_parity_group_id = stripe->z;
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < stripe->k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / h);
      }
      else if (i < stripe->k + stripe->r)
      {
        std::string tmp = "_G";
        if (i - stripe->k < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = global_parity_group_id;
      }
      else
      {
        std::string tmp = "_L";
        if (i - stripe->k - stripe->r < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i - stripe->k - stripe->r);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - stripe->k - stripe->r;
      }
    }

    std::vector<int> assigned_cluster(stripe->n, -1);

    // Step 1: global parity blocks
    for (int i = stripe->k; i < stripe->k + stripe->r; ++i)
    {
      assigned_cluster[i] = global_cluster;
    }

    const int n_primary_data = std::min(stripe->r, h);
    std::vector<std::vector<int>> group_remainder_blocks(stripe->z);

    for (int g = 0; g < stripe->z; ++g)
    {
      const int base = g * h;
      const int local_parity_id = stripe->k + stripe->r + g;

      // Step 2: local parity + min(r,h) data blocks
      const int primary_cluster = next_non_global_cluster();
      for (int j = 0; j < n_primary_data; ++j)
      {
        assigned_cluster[base + j] = primary_cluster;
      }
      assigned_cluster[local_parity_id] = primary_cluster;

      int next_idx = base + n_primary_data;

      // Step 3: one data block to global cluster
      if (next_idx < base + h)
      {
        assigned_cluster[next_idx] = global_cluster;
        next_idx++;
      }

      // Step 4: batches of (r+1) data blocks
      const int batch_size = stripe->r + 1;
      while (next_idx + batch_size <= base + h)
      {
        const int batch_cluster = next_non_global_cluster();
        for (int j = 0; j < batch_size; ++j)
        {
          assigned_cluster[next_idx] = batch_cluster;
          next_idx++;
        }
      }

      // Step 5 leftovers for this group (m blocks when equal across groups)
      while (next_idx < base + h)
      {
        group_remainder_blocks[g].push_back(next_idx);
        next_idx++;
      }
    }

    // Step 5: merge theta local groups' remainders into one cluster when m is equal
    int m = -1;
    bool equal_m = true;
    for (int g = 0; g < stripe->z; ++g)
    {
      const int gm = static_cast<int>(group_remainder_blocks[g].size());
      if (m < 0)
      {
        m = gm;
      }
      else if (gm != m)
      {
        equal_m = false;
        break;
      }
    }

    if (m > 0)
    {
      if (equal_m)
      {
        int theta = 1;
        if (m == 1)
        {
          theta = stripe->r + 1;
        }
        else
        {
          theta = stripe->r / (m - 1);
          if (theta < 1)
          {
            theta = 1;
          }
        }

        for (int g = 0; g < stripe->z; g += theta)
        {
          const int batch_groups = std::min(theta, stripe->z - g);
          const int remainder_cluster = next_non_global_cluster();
          for (int gi = 0; gi < batch_groups; ++gi)
          {
            for (int block_idx : group_remainder_blocks[g + gi])
            {
              assigned_cluster[block_idx] = remainder_cluster;
            }
          }
        }
      }
      else
      {
        for (int g = 0; g < stripe->z; ++g)
        {
          if (group_remainder_blocks[g].empty())
          {
            continue;
          }
          const int remainder_cluster = next_non_global_cluster();
          for (int block_idx : group_remainder_blocks[g])
          {
            assigned_cluster[block_idx] = remainder_cluster;
          }
        }
      }
    }

    for (int i = 0; i < stripe->n; ++i)
    {
      if (assigned_cluster[i] < 0)
      {
        throw std::runtime_error("CordXueLRC placement failed: unassigned block");
      }
    }

    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].map2cluster = assigned_cluster[i];
      int t_node_id = randomly_select_a_node(blocks_info[i].map2cluster, stripe->stripe_id, gen);
      blocks_info[i].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, i);
      m_cluster_table[blocks_info[i].map2cluster].blocks.push_back(&blocks_info[i]);
      m_cluster_table[blocks_info[i].map2cluster].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[i]);
      stripe->place2clusters.insert(blocks_info[i].map2cluster);
      add_to_map(stripe->group_to_blocks, blocks_info[i].map2group, i);
    }

    stripe->num_groups = stripe->group_to_blocks.size();
  }

  void CoordinatorImpl::add_to_map(std::map<int, std::vector<int>> &map, int key, int value)
  {
    if (map.find(key) == map.end())
      map[key] = std::vector<int>();
    map[key].push_back(value);
  }

  int CoordinatorImpl::getClusterAppendSize(Stripe *stripe, const std::map<int, std::pair<int, int>> &block_to_slice_sizes, int curr_group_id, int parity_slice_size)
  {
    int cluster_append_size = 0;

    for (int i = curr_group_id * stripe->k / stripe->z; i < (curr_group_id + 1) * stripe->k / stripe->z; i++)
    {
      if (block_to_slice_sizes.find(i) != block_to_slice_sizes.end())
        cluster_append_size += block_to_slice_sizes.at(i).first;
    }

    cluster_append_size += parity_slice_size * (stripe->r + stripe->z) / stripe->z;
    return cluster_append_size;
  }

  // add repeated fields to plan
  void addBlockToAppendPlan(proxy_proto::AppendStripeDataPlacement &plan,
                            const Block *block,
                            const Node &node,
                            const std::pair<int, int> &slice_info)
  {
    plan.add_datanodeip(node.node_ip);
    plan.add_datanodeport(node.node_port);
    plan.add_blockkeys(block->block_key);
    plan.add_blockids(block->block_id);
    plan.add_offsets(slice_info.second);
    plan.add_sizes(slice_info.first);
  }

  bool CoordinatorImpl::build_slice_plan_for_logical_range(
      Stripe *stripe,
      int curr_logical_offset,
      int append_size,
      std::map<int, std::pair<int, int>> *out_block_to_slice_sizes,
      int *out_parity_slice_size,
      int *out_parity_slice_offset,
      bool *out_is_merge_parity,
      std::string *err_msg)
  {
    if (stripe == nullptr || out_block_to_slice_sizes == nullptr || out_parity_slice_size == nullptr ||
        out_parity_slice_offset == nullptr || out_is_merge_parity == nullptr || err_msg == nullptr)
    {
      if (err_msg)
      {
        *err_msg = "null argument";
      }
      return false;
    }
    out_block_to_slice_sizes->clear();
    const int unit_size = static_cast<int>(m_sys_config->UnitSize);
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int stripe_data_bytes = stripe->k * block_size;
    if (append_size < 0 || curr_logical_offset < 0 || curr_logical_offset > stripe_data_bytes ||
        curr_logical_offset + append_size > stripe_data_bytes)
    {
      *err_msg = "logical offset/length out of stripe data range";
      return false;
    }

    // 新语义：输入区间直接按“条带内顺序拼接的数据块地址空间”映射：
    // block_id = logical_offset / BlockSize, block_offset = logical_offset % BlockSize。
    const int logical_end = curr_logical_offset + append_size - 1;
    int pos = curr_logical_offset;
    int min_unit_idx = std::numeric_limits<int>::max();
    int max_unit_idx = -1;
    while (pos <= logical_end)
    {
      const int block_id = pos / block_size;
      const int block_offset = pos % block_size;
      const int block_tail = block_size - block_offset;
      const int len = std::min(block_tail, logical_end - pos + 1);
      (*out_block_to_slice_sizes)[block_id] = std::make_pair(len, block_offset);

      const int unit_begin = block_offset / unit_size;
      const int unit_end = (block_offset + len - 1) / unit_size;
      min_unit_idx = std::min(min_unit_idx, unit_begin);
      max_unit_idx = std::max(max_unit_idx, unit_end);
      pos += len;
    }

    if (min_unit_idx > max_unit_idx)
    {
      *err_msg = "empty mapped data slices";
      return false;
    }

    const int parity_slice_offset = min_unit_idx * unit_size;
    const int parity_slice_size = std::min(block_size - parity_slice_offset, (max_unit_idx - min_unit_idx + 1) * unit_size);
    for (int i = stripe->k; i < stripe->n; i++)
    {
      (*out_block_to_slice_sizes)[i] = std::make_pair(parity_slice_size, parity_slice_offset);
    }

    *out_is_merge_parity = (curr_logical_offset == 0 && append_size == stripe_data_bytes);
    *out_parity_slice_size = parity_slice_size;
    *out_parity_slice_offset = parity_slice_offset;
    err_msg->clear();
    return true;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generateAppendPlan(Stripe *stripe, int curr_logical_offset, int append_size)
  {
    std::vector<proxy_proto::AppendStripeDataPlacement> append_plans;
    std::string append_mode = m_sys_config->AppendMode;
    int remain_size = stripe->k * m_sys_config->BlockSize - curr_logical_offset;
    assert(remain_size >= append_size && "append size is larger than the remaining size of the stripe!");

    std::map<int, std::pair<int, int>> block_to_slice_sizes;
    int parity_slice_size = -1;
    int parity_slice_offset = -1;
    bool is_merge_parity = false;
    std::string err;
    if (!build_slice_plan_for_logical_range(stripe, curr_logical_offset, append_size, &block_to_slice_sizes,
                                            &parity_slice_size, &parity_slice_offset, &is_merge_parity, &err))
    {
      std::cout << "[ERROR] " << err << std::endl;
      return append_plans;
    }

    for (int i = 0; i < stripe->z; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(getClusterAppendSize(stripe, block_to_slice_sizes, i, parity_slice_size));
      plan.set_is_merge_parity(is_merge_parity);
        plan.set_cluster_id(stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster);
      plan.set_append_mode(append_mode);
      if (curr_logical_offset == 0 && append_size == m_sys_config->BlockSize * stripe->k)
      {
        plan.set_is_serialized(false);
        plan.set_is_merge_parity(false);
      }
      else
      {
        plan.set_is_serialized(true);
      }

      // Add data slices to plan
      for (int j = i * stripe->k / stripe->z;
           j < (i + 1) * stripe->k / stripe->z; j++)
      {
        if (block_to_slice_sizes.find(j) != block_to_slice_sizes.end())
        {
          addBlockToAppendPlan(plan, stripe->blocks[j],
                               m_node_table[stripe->blocks[j]->map2node],
                               block_to_slice_sizes.at(j));
        }
      }

      // Add global parity slices to plan
      for (int j = stripe->k + i * stripe->r / stripe->z;
           j < stripe->k + (i + 1) * stripe->r / stripe->z; j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[j],
                             m_node_table[stripe->blocks[j]->map2node],
                             block_to_slice_sizes.at(j));
      }

      // Add local parity slices to plan
      for (int j = stripe->k + stripe->r + i * stripe->z / stripe->z;
           j < stripe->k + stripe->r + (i + 1) * stripe->z / stripe->z; j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[j],
                             m_node_table[stripe->blocks[j]->map2node],
                             block_to_slice_sizes.at(j));
      }

      append_plans.push_back(plan);
    }

    return append_plans;
  }

  bool CoordinatorImpl::notify_proxies_cord_ready(const proxy_proto::CordDataUpdatePlacement &plan)
  {
    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    const int cid = plan.cluster_id();
    std::string chosen_proxy =
        m_cluster_table[cid].proxy_ip + ":" + std::to_string(m_cluster_table[cid].proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->scheduleCordDataUpdate(&cont, plan, &set_reply);
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[plan.key()] =
          ObjectInfo(static_cast<int>(plan.update_payload_size()), plan.stripe_id());
      m_mutex.unlock();
      return true;
    }
    std::cout << "[CoRD] scheduleCordDataUpdate key=" << plan.key() << " failed: " << status.error_message()
              << std::endl;
    return false;
  }

  void CoordinatorImpl::notify_proxies_cord_transfer_plan(const proxy_proto::CordTransferPlan &plan)
  {
    if (plan.steps_size() <= 0)
    {
      return;
    }
    std::set<int> clusters;
    for (int i = 0; i < plan.steps_size(); ++i)
    {
      clusters.insert(plan.steps(i).src_proxy_cluster_id());
      clusters.insert(plan.steps(i).dst_proxy_cluster_id());
    }
  // Phase 1: register plan on every involved proxy before any execution starts.
    const std::vector<int> cluster_list(clusters.begin(), clusters.end());
    auto notify_one_cluster = [this](int cid, auto rpc_fn) {
      if (cid < 0)
        return;
      auto cit = m_cluster_table.find(cid);
      if (cit == m_cluster_table.end())
        return;
      const std::string pkey = cit->second.proxy_ip + ":" + std::to_string(cit->second.proxy_port);
      auto pit = m_proxy_ptrs.find(pkey);
      if (pit == m_proxy_ptrs.end() || !pit->second)
      {
        std::cout << "[CoRD-PLAN] no proxy stub for cluster " << cid << " (" << pkey << ")" << std::endl;
        return;
      }
      grpc::ClientContext ctx;
      proxy_proto::SetReply rep;
      grpc::Status st = rpc_fn(pit->second.get(), &ctx, &rep);
      if (!st.ok() || !rep.ifcommit())
      {
        std::cout << "[CoRD-PLAN] transfer plan notify failed cluster " << cid << " st=" << st.error_message()
                  << std::endl;
      }
    };

    std::vector<std::thread> notify_threads;
    notify_threads.reserve(cluster_list.size());
    for (int cid : cluster_list)
    {
      notify_threads.emplace_back([this, &plan, cid, &notify_one_cluster]() {
        notify_one_cluster(cid, [&](auto *stub, grpc::ClientContext *ctx, proxy_proto::SetReply *rep) {
          return stub->scheduleCordTransferPlan(ctx, plan, rep);
        });
      });
    }
    for (auto &th : notify_threads)
      th.join();

  // Phase 2: start execution on all proxies (all plan_key registrations are visible).
    proxy_proto::CordPlanKeyMsg start_msg;
    start_msg.set_plan_key(plan.plan_key());
    notify_threads.clear();
    notify_threads.reserve(cluster_list.size());
    for (int cid : cluster_list)
    {
      notify_threads.emplace_back([this, &start_msg, cid, &notify_one_cluster]() {
        notify_one_cluster(cid, [&](auto *stub, grpc::ClientContext *ctx, proxy_proto::SetReply *rep) {
          return stub->cordPlanStartExecution(ctx, start_msg, rep);
        });
      });
    }
    for (auto &th : notify_threads)
      th.join();
  }


  bool CoordinatorImpl::cord_start_pending_transfer_plan(const std::string &plan_key)
  {
    proxy_proto::CordTransferPlan plan;
    {
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      auto it = m_cord_pending_plans.find(plan_key);
      if (it == m_cord_pending_plans.end())
        return false;
      plan = it->second;
      m_cord_pending_plans.erase(it);
    }
    if (cord_trace_log(IF_DEBUG))
      std::cout << "[CoRD] start CordTransferPlan: plan_key=" << plan_key
                << " steps=" << plan.steps_size() << " rounds=" << plan.total_rounds() << "\n";
    notify_proxies_cord_transfer_plan(plan);
    return true;
  }

  void CoordinatorImpl::cord_register_auto_begin_session(const std::string &plan_key,
                                                         const std::vector<std::string> &delta_append_keys)
  {
    std::lock_guard<std::mutex> lk(m_cord_pending_mu);
    CordAutoBeginSession session;
    session.transfer_started = false;
    for (const auto &k : delta_append_keys)
    {
      session.pending_delta_keys.insert(k);
      m_cord_append_key_to_plan_key[k] = plan_key;
    }
    m_cord_auto_begin_sessions[plan_key] = std::move(session);
  }

  void CoordinatorImpl::cord_on_delta_key_committed(const std::string &delta_append_key)
  {
    std::string plan_key;
    bool should_start = false;
    {
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      auto kit = m_cord_append_key_to_plan_key.find(delta_append_key);
      if (kit == m_cord_append_key_to_plan_key.end())
        return;
      plan_key = kit->second;
      auto sit = m_cord_auto_begin_sessions.find(plan_key);
      if (sit == m_cord_auto_begin_sessions.end())
        return;
      sit->second.pending_delta_keys.erase(delta_append_key);
      if (!sit->second.pending_delta_keys.empty() || sit->second.transfer_started)
        return;
      sit->second.transfer_started = true;
      should_start = m_cord_pending_plans.find(plan_key) != m_cord_pending_plans.end();
    }
    if (should_start)
    {
      if (cord_trace_log(IF_DEBUG))
        std::cout << "[CoRD] all delta uploads committed, auto-start transfer plan_key=" << plan_key << "\n";
      cord_start_pending_transfer_plan(plan_key);
    }
  }

  void CoordinatorImpl::cord_clear_auto_begin_session(const std::string &plan_key)
  {
    std::lock_guard<std::mutex> lk(m_cord_pending_mu);
    auto sit = m_cord_auto_begin_sessions.find(plan_key);
    if (sit == m_cord_auto_begin_sessions.end())
      return;
    for (const auto &k : sit->second.pending_delta_keys)
      m_cord_append_key_to_plan_key.erase(k);
    m_cord_auto_begin_sessions.erase(sit);
  }

  void CoordinatorImpl::enrich_cord_transfer_plan_encoding(
      Stripe *stripe,
      const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
      const cord_alg2::Algorithm2Result &alg2,
      proxy_proto::CordTransferPlan *plan)
  {
    plan->clear_cord_encode_meta();
    plan->clear_cord_collector_expects();
    plan->clear_cord_data_strip_descs();
    if (stripe == nullptr || plan == nullptr)
      return;
    const int k = stripe->k;
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    int global_lo = std::numeric_limits<int>::max();
    int global_hi_excl = std::numeric_limits<int>::min();
    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      for (const auto &seg : kv.second)
      {
        const int lo = bid * block_size + seg.first;
        const int hi_excl = bid * block_size + seg.second;
        global_lo = std::min(global_lo, lo);
        global_hi_excl = std::max(global_hi_excl, hi_excl);
      }
    }
    if (global_lo >= global_hi_excl)
      return;
    const int append_size = global_hi_excl - global_lo;
    std::map<int, std::pair<int, int>> b2s;
    int ps = 0, po = 0;
    bool merge = false;
    std::string err;
    if (!build_slice_plan_for_logical_range(stripe, global_lo, append_size, &b2s, &ps, &po, &merge, &err))
    {
      std::cout << "[CoRD] enrich_cord_transfer_plan_encoding build_slice failed: " << err << std::endl;
      return;
    }
    proxy_proto::CordTransferEncodeMeta *meta = plan->mutable_cord_encode_meta();
    meta->set_encode_type(static_cast<int32_t>(m_encode_parameters.encodetype));
    meta->set_k(stripe->k);
    // CoRD SET 路径只初始化 stripe->r/z，g_m/l 可能未赋值；与 proxy ingress 矩阵编码一致用 r/z
    meta->set_g_m(m_sys_config->r);
    meta->set_l(m_sys_config->z);
    meta->set_parity_slice_offset(po);
    meta->set_parity_slice_size(ps);

    for (const auto &kv : b2s)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      proxy_proto::CordDataStripDesc *d = plan->add_cord_data_strip_descs();
      d->set_block_id(bid);
      d->set_slice_offset(kv.second.second);
      d->set_slice_len(kv.second.first);
    }

    std::map<std::pair<int, int>, std::map<int, uint64_t>> coll_agg;
    for (const auto &L : alg2.train_route)
    {
      if (L.kind != cord_alg2::TrainLinkKind::STAR_DATA_TO_CENTER)
        continue;
      if (L.delta_kind != cord_alg2::CordDeltaPayloadKind::DATA_DELTA)
        continue;
      const std::pair<int, int> key(L.group_index, L.dst_block_id);
      const uint64_t ext = cord_block_delta_ingress_buffer_end(block_intervals, L.src_block_id);
      auto &m = coll_agg[key];
      auto it = m.find(L.src_block_id);
      if (it == m.end() || ext > it->second)
        m[L.src_block_id] = ext;
    }
    for (const auto &kv : coll_agg)
    {
      proxy_proto::CordCollectorIngressExpect *ex = plan->add_cord_collector_expects();
      ex->set_group_index(kv.first.first);
      ex->set_collector_block_id(kv.first.second);
      for (const auto &src_kv : kv.second)
      {
        ex->add_src_data_block_ids(src_kv.first);
        ex->add_src_delta_total_bytes(src_kv.second);
      }
    }
  }

  void CoordinatorImpl::notify_proxies_ready(const proxy_proto::AppendStripeDataPlacement &plan)
  {
    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    std::string chosen_proxy = m_cluster_table[plan.cluster_id()].proxy_ip + ":" + std::to_string(m_cluster_table[plan.cluster_id()].proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->scheduleAppend2Datanode(&cont, plan, &set_reply);
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[plan.key()] = ObjectInfo(plan.append_size(), plan.stripe_id());
      m_mutex.unlock();
    }
    else
    {
      std::cout << "[APPEND434] Send append plan" << plan.key() << " failed! " << std::endl;
    }
  }

  // Only processing the appending within a single stripe
  grpc::Status CoordinatorImpl::uploadAppendValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    int appendSizeBytes = keyValueSize->valuesizebytes();
    std::string append_mode = keyValueSize->append_mode();
    std::string code_type = m_sys_config->CodeType;

    // 1. record metadata
    // logical offset within the block stripe
    if (m_cur_offset_table.find(clientID) == m_cur_offset_table.end())
    {
      // first append
      m_cur_offset_table[clientID] = StripeOffset(m_cur_stripe_id++, 0);
    }
    StripeOffset curStripeOffset = m_cur_offset_table[clientID];

    assert(curStripeOffset.offset + appendSizeBytes <= m_sys_config->BlockSize * m_sys_config->k && "append size is larger than the remaining size of the stripe!");

    // 2. generate data placement
    Stripe *stripe = nullptr;
    if (curStripeOffset.offset == 0)
    {
      // first append
      Stripe t_stripe;
      t_stripe.stripe_id = curStripeOffset.stripe_id;
      t_stripe.n = m_sys_config->n;
      t_stripe.k = m_sys_config->k;
      t_stripe.r = m_sys_config->r;
      t_stripe.z = m_sys_config->z;
      t_stripe.g_m = m_sys_config->r;
      t_stripe.l = m_sys_config->z;
      t_stripe.object_keys.push_back(clientID);
      if (code_type == "UniLRC" || code_type == "AzureLRC")
      {
        initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
      }
      else if (code_type == "OptimalLRC")
      {
        initialize_optimal_lrc_stripe_placement(&t_stripe);
      }
      else if (code_type == "UniformLRC")
      {
        initialize_uniform_lrc_stripe_placement(&t_stripe);
      }
      else if (code_type == "RandomLRC")
      {
        initialize_random_lrc_stripe_placement(&t_stripe);
      }
      else if (code_type == "SplitParityLRC")
      {
        initialize_split_parity_lrc_stripe_placement(&t_stripe);
      }
      else if (code_type == "CordXueLRC")
      {
        initialize_cord_xue_lrc_stripe_placement(&t_stripe);
      }
      else
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Unsupported code type");
      }
      m_stripe_table[t_stripe.stripe_id] = t_stripe;
      stripe = &m_stripe_table[t_stripe.stripe_id];
    }
    else
    {
      // append to the existing stripe
      stripe = &m_stripe_table[curStripeOffset.stripe_id];
    }

    std::vector<proxy_proto::AppendStripeDataPlacement> append_plans = generateAppendPlan(stripe, curStripeOffset.offset, appendSizeBytes);
    if (append_plans.empty())
    {
      std::cout << "[ERROR] Invalid append mode: " << append_mode << std::endl;
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid append mode");
    }

    for (const auto &plan : append_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    // 3. 并行通知所有 proxy 接收数据
    int sum_append_size = 0;
    {
      const int plan_count = static_cast<int>(append_plans.size());
      std::vector<std::thread> notify_threads;
      notify_threads.reserve(static_cast<size_t>(plan_count));
      for (int i = 0; i < plan_count; ++i)
      {
        notify_threads.emplace_back([this, &plan = append_plans[static_cast<size_t>(i)]]() {
          notify_proxies_ready(plan);
        });
      }
      for (auto &t : notify_threads)
        t.join();
    }

    // 串行填充 proxyIPPort（protobuf 非线程安全）
    for (const auto &plan : append_plans)
    {
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::SET_XFER_PORT_OFFSET); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_cur_offset_table[clientID].offset += appendSizeBytes;
    // std::cout << "[Coordinator] stripe_id: " << m_cur_offset_table[clientID].stripe_id << " offset: " << m_cur_offset_table[clientID].offset << " is_erase " << (m_cur_offset_table[clientID].offset == m_sys_config->BlockSize * m_sys_config->k) << std::endl;
    if (m_cur_offset_table[clientID].offset == m_sys_config->BlockSize * m_sys_config->k)
    {
      m_cur_offset_table.erase(clientID);
    }

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadXueUpdate(
      grpc::ServerContext *context,
      const coordinator_proto::XueUpdateRequest *request,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    (void)context;
    (void)request;
    (void)proxyIPPort;
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "XueLRC update strategy has been removed");
  }

  grpc::Status CoordinatorImpl::uploadCordUpdate(
      grpc::ServerContext *context,
      const coordinator_proto::CordUpdateRequest *request,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    (void)context;
    proxyIPPort->Clear();

    const int stripe_id = request->stripe_id();
    if (m_stripe_table.find(stripe_id) == m_stripe_table.end())
    {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "stripe_id not found");
    }
    Stripe *stripe = &m_stripe_table[stripe_id];
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int k = stripe->k;
    const int stripe_data_bytes = k * block_size;

    if (request->interval_count() > 0 &&
        request->interval_count() != request->update_intervals_size())
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "interval_count does not match update_intervals size");
    }

    if (request->update_intervals_size() == 0)
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty update_intervals");
    }

    std::map<int, std::vector<std::pair<int, int>>> block_intervals;

    for (int ri = 0; ri < request->update_intervals_size(); ++ri)
    {
      const auto &r = request->update_intervals(ri);
      const int s = r.logical_offset_start();
      const int e = r.logical_offset_end();
      if (e <= s)
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "invalid half-open interval [start,end)");
      }
      if (s < 0 || s > stripe_data_bytes || e > stripe_data_bytes)
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "logical interval out of stripe data range");
      }
      cord_add_logical_range_to_data_blocks(block_size, k, s, e, &block_intervals);
    }

    // Flip offsets for even-numbered data blocks: mirror the update range within the block.
    // e.g., a range at the last 8 KB of the block → first 8 KB of the block.
    for (auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid % 2 == 0)
      {
        for (auto &seg : kv.second)
        {
          const int lo = seg.first;
          const int hi = seg.second;
          seg.first  = block_size - hi;
          seg.second = block_size - lo;
        }
        // restore ascending order after flipping
        std::sort(kv.second.begin(), kv.second.end());
      }
    }

    // --- CoRD uploadCordUpdate verbose debug (IF_DEBUG or CORD_VERBOSE=1) ---
    const bool cord_trace = cord_trace_log(IF_DEBUG);
    if (cord_trace)
    {
      std::cout << "[CoRD] ===== uploadCordUpdate stripe_id=" << stripe_id
                << " k=" << k << " r=" << stripe->r << " z=" << stripe->z
                << " n=" << stripe->n << " block_size=" << block_size
                << " n_intervals=" << request->update_intervals_size() << " =====\n";
      std::cout << "[CoRD] stripe_id=" << stripe_id
                << " updated data blocks (block_id -> in-block intervals [off,end)):\n";
      for (const auto &kv : block_intervals)
      {
        std::cout << "  block " << kv.first << " (cluster c" << stripe->blocks[kv.first]->map2cluster << "):";
        for (const auto &seg : kv.second)
        {
          std::cout << " [" << seg.first << "," << seg.second << ")";
        }
        std::cout << "\n";
      }
    }

    std::map<int, cord_class::CordIngressClusterHints> ingress_hints_by_cluster;

    cord_alg2::Algorithm2Result alg2_result;
    {
      cord_alg2::TransferParams tp;
      tp.enforce_one_send_one_recv_per_cluster = false;
      const std::vector<std::string> bw_paths = {
          "/root/xue/project/config/BW_limitsame",
          "project/config/BW_limitsame",
          "../project/config/BW_limitsame",
      };
      bool bw_ok = false;
      for (const auto &p : bw_paths)
      {
        if (cord_alg2::load_bw_matrix_from_limitsame_file(p, m_sys_config->ClusterNum, &tp))
        {
          bw_ok = true;
          break;
        }
      }
      if (!bw_ok && cord_trace)
        std::cout << "[CoRD-Class] BW matrix load failed, using fallback inv_bw\n";
      alg2_result = cord_class::build_class_update_plan(*stripe, block_intervals, m_sys_config->ClusterNum, tp,
                                                        &ingress_hints_by_cluster);
      if (cord_trace)
      {
        std::cout << "[CoRD-Class] train_route links=" << alg2_result.train_route.size()
                  << " schedule_steps=" << alg2_result.timeslot_schedule.size();
        if (alg2_result.center_global_block_id >= 0)
          std::cout << " collector_blk=" << alg2_result.center_global_block_id;
        std::cout << "\n";
        for (size_t i = 0; i < alg2_result.train_route.size(); ++i)
        {
          const auto &L = alg2_result.train_route[i];
          std::cout << "  [" << i << "] " << cord_alg2::train_link_kind_name(L.kind)
                    << " blk" << L.src_block_id << "->blk" << L.dst_block_id << " c" << L.src_cluster << "->c"
                    << L.dst_cluster << " bytes=" << L.payload_bytes << " grp=" << L.group_index
                    << " delta=" << (L.delta_kind == cord_alg2::CordDeltaPayloadKind::PARITY_DELTA ? "ΔP" : "ΔD")
                    << "\n";
        }
        for (const auto &ts : alg2_result.timeslot_schedule)
        {
          std::cout << "  slot " << ts.timeslot << ": links=[";
          for (size_t li = 0; li < ts.link_indices.size(); ++li)
          {
            if (li > 0)
              std::cout << " ";
            std::cout << ts.link_indices[li];
          }
          std::cout << "]\n";
        }
      }
    }

    proxy_proto::CordTransferPlan cord_xfer_plan;
    std::string cord_xfer_plan_key;
    {
      cord_xfer_plan_key = std::string("cord_xfer_") + std::to_string(stripe_id) + "_" +
                           std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now().time_since_epoch())
                                              .count());
      cord_xfer_plan = cord_transfer_plan_from_algorithm2(stripe_id, cord_xfer_plan_key, alg2_result, stripe->k,
                                                            block_intervals);
      if (cord_trace)
      {
        std::cout << "[CoRD] CordTransferPlan: steps=" << cord_xfer_plan.steps_size()
                  << " schedule_steps=" << cord_xfer_plan.total_rounds() << "\n";
      }
      enrich_cord_transfer_plan_delta_segs(block_intervals, stripe->k, &cord_xfer_plan);
      fill_group_xor_hints_from_alg2(alg2_result, block_intervals, &cord_xfer_plan);
      enrich_cord_transfer_plan_encoding(stripe, block_intervals, alg2_result, &cord_xfer_plan);
      enrich_cord_transfer_plan_block_stripe_groups(stripe, &cord_xfer_plan);
      enrich_cord_transfer_plan_step_parity_filters(stripe, &cord_xfer_plan);
    }

    std::map<int, std::vector<CordSliceRec>> cluster_slices;
    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      Block *bp = stripe->blocks[bid];
      for (const auto &seg : kv.second)
      {
        CordSliceRec r;
        r.block_id = bid;
        r.block_offset = seg.first;
        r.len = seg.second - seg.first;
        if (r.len <= 0)
          continue;
        cluster_slices[bp->map2cluster].push_back(r);
      }
    }
    for (auto &cs : cluster_slices)
    {
      std::sort(cs.second.begin(), cs.second.end(),
                [](const CordSliceRec &a, const CordSliceRec &b)
                {
                  if (a.block_id != b.block_id)
                    return a.block_id < b.block_id;
                  return a.block_offset < b.block_offset;
                });
    }

    uint64_t sum_update_bytes = 0;
    for (const auto &cs : cluster_slices)
      for (const auto &s : cs.second)
        sum_update_bytes += static_cast<uint64_t>(s.len);

    if (cluster_slices.empty() || sum_update_bytes == 0)
    {
      proxyIPPort->set_sum_append_size(0);
      return grpc::Status::OK;
    }

    std::vector<std::pair<int, std::vector<CordSliceRec>>> sorted_clusters(cluster_slices.begin(),
                                                                             cluster_slices.end());
    std::sort(sorted_clusters.begin(), sorted_clusters.end(),
              [](const std::pair<int, std::vector<CordSliceRec>> &a,
                 const std::pair<int, std::vector<CordSliceRec>> &b)
              { return a.first < b.first; });

    enrich_cord_transfer_plan_topology(stripe, m_cluster_table, m_node_table, m_toolbox, &cord_xfer_plan,
                                       sorted_clusters);
    reorder_cord_plan_steps_execution(&cord_xfer_plan);

    if (cord_trace && cord_xfer_plan.steps_size() > 0) {
      std::cout << "[CoRD] CordTransferPlan final execution order (" << cord_xfer_plan.steps_size() << " steps):\n";
      for (int si = 0; si < cord_xfer_plan.steps_size(); ++si) {
        const auto &st = cord_xfer_plan.steps(si);
        std::cout << "  step[" << si << "] slot=" << st.scheduled_slot()
                  << " c" << st.src_proxy_cluster_id() << "→c" << st.dst_proxy_cluster_id()
                  << " blk" << st.src_block_id() << "→blk" << st.dst_block_id()
                  << " chunk[" << st.chunk_byte_offset() << "+" << st.chunk_byte_length() << "B]"
                  << " link=" << static_cast<int>(st.link_kind())
                  << " delta=" << (st.delta_payload_kind() == proxy_proto::CORD_DELTA_PARITY ? "ΔP" : "ΔD")
                  << " grp=" << st.group_index()
                  << " dep=" << st.depends_on_step_index() << "\n";
      }
    }

    if (cord_trace)
    {
      std::cout << "[CoRD] Delta store dispatch: " << sorted_clusters.size() << " clusters (parallel notify)\n";
    }
    struct CordDeltaNotifyJob {
      proxy_proto::CordDataUpdatePlacement plan;
      int cid = -1;
      uint64_t cluster_payload = 0;
      bool ok = false;
    };
    std::vector<CordDeltaNotifyJob> notify_jobs;
    notify_jobs.reserve(sorted_clusters.size());
    for (const auto &plan_entry : sorted_clusters)
    {
      const int cid = plan_entry.first;
      const auto &slices = plan_entry.second;
      if (m_cluster_table.find(cid) == m_cluster_table.end() ||
          m_cluster_table[cid].nodes.empty())
      {
        return grpc::Status(grpc::StatusCode::INTERNAL, "cluster has no datanode for CoRD delta store");
      }
      CordDeltaNotifyJob job;
      job.cid = cid;
      proxy_proto::CordDataUpdatePlacement &plan = job.plan;
      plan.set_key(m_toolbox->gen_cord_key(stripe_id, cid));
      plan.set_cluster_id(cid);
      plan.set_stripe_id(stripe_id);
      for (const auto &s : slices)
        job.cluster_payload += static_cast<uint64_t>(s.len);
      plan.set_update_payload_size(job.cluster_payload);
      const int delta_node_id = m_cluster_table[cid].nodes.front();
      const Node &delta_node = m_node_table[delta_node_id];
      plan.set_delta_blob_key(plan.key() + "_delta");
      plan.set_delta_datanode_ip(delta_node.node_ip);
      plan.set_delta_datanode_port(delta_node.node_port);
      for (const auto &s : slices)
      {
        Block *b = stripe->blocks[s.block_id];
        const Node &n = m_node_table[b->map2node];
        plan.add_datanodeip(n.node_ip);
        plan.add_datanodeport(n.node_port);
        plan.add_blockkeys(b->block_key);
        plan.add_blockids(b->block_id);
        plan.add_offsets(static_cast<uint64_t>(s.block_offset));
        plan.add_sizes(static_cast<uint64_t>(s.len));
      }

      auto ihit = ingress_hints_by_cluster.find(cid);
      if (ihit != ingress_hints_by_cluster.end())
      {
        for (const auto &wr : ihit->second.local_parity_writes)
        {
          if (wr.block_id < 0 || wr.block_id >= static_cast<int>(stripe->blocks.size()))
            continue;
          Block *bp = stripe->blocks[wr.block_id];
          const Node &n = m_node_table[bp->map2node];
          proxy_proto::CordIngressParityWrite *pw = plan.add_cord_ingress_local_parity_writes();
          pw->set_block_id(wr.block_id);
          pw->set_block_key(bp->block_key);
          pw->set_datanode_ip(n.node_ip);
          pw->set_datanode_port(n.node_port);
          pw->set_stripe_group(wr.stripe_group);
        }
        for (const auto &wr : ihit->second.global_parity_writes)
        {
          if (wr.block_id < 0 || wr.block_id >= static_cast<int>(stripe->blocks.size()))
            continue;
          Block *bp = stripe->blocks[wr.block_id];
          const Node &n = m_node_table[bp->map2node];
          proxy_proto::CordIngressParityWrite *pw = plan.add_cord_ingress_global_parity_writes();
          pw->set_block_id(wr.block_id);
          pw->set_block_key(bp->block_key);
          pw->set_datanode_ip(n.node_ip);
          pw->set_datanode_port(n.node_port);
          pw->set_stripe_group(wr.stripe_group);
        }
        for (int32_t sg : ihit->second.cache_lp_stripe_groups)
          plan.add_cord_ingress_cache_lp_stripe_groups(sg);
      }

      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();

      notify_jobs.push_back(std::move(job));
    }

    std::vector<std::thread> notify_threads;
    notify_threads.reserve(notify_jobs.size());
    for (auto &job : notify_jobs)
    {
      notify_threads.emplace_back([this, &job]() {
        job.ok = notify_proxies_cord_ready(job.plan);
      });
    }
    for (auto &th : notify_threads)
      th.join();

    std::vector<std::string> cord_delta_append_keys;
    cord_delta_append_keys.reserve(notify_jobs.size());
    for (const auto &job : notify_jobs)
    {
      if (!job.ok)
      {
        return grpc::Status(grpc::StatusCode::INTERNAL,
                            "scheduleCordDataUpdate failed for cluster " + std::to_string(job.cid) +
                                " key=" + job.plan.key());
      }
      proxyIPPort->add_append_keys(job.plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[job.cid].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[job.cid].proxy_port + ECProject::PROXY_PORT_SHIFT);
      proxyIPPort->add_cluster_slice_sizes(job.cluster_payload);
      proxyIPPort->add_group_ids(job.cid);
      cord_delta_append_keys.push_back(job.plan.key());
    }
    proxyIPPort->set_sum_append_size(sum_update_bytes);
    if (cord_xfer_plan.steps_size() > 0)
    {
      std::set<int> plan_clusters;
      for (int si = 0; si < cord_xfer_plan.steps_size(); ++si)
      {
        const auto &st = cord_xfer_plan.steps(si);
        if (st.src_proxy_cluster_id() >= 0)
          plan_clusters.insert(st.src_proxy_cluster_id());
        if (st.dst_proxy_cluster_id() >= 0)
          plan_clusters.insert(st.dst_proxy_cluster_id());
      }
      {
        std::lock_guard<std::mutex> lk(m_cord_pending_mu);
        m_cord_pending_plans[cord_xfer_plan_key] = cord_xfer_plan;
        m_cord_pending_plan_clusters[cord_xfer_plan_key].assign(plan_clusters.begin(), plan_clusters.end());
      }
      proxyIPPort->set_cord_transfer_plan_key(cord_xfer_plan_key);
      if (cord_trace)
      {
        std::cout << "[CoRD] CordTransferPlan registered: key=" << cord_xfer_plan_key
                  << " steps=" << cord_xfer_plan.steps_size()
                  << " rounds=" << cord_xfer_plan.total_rounds()
                  << " plan_clusters=" << plan_clusters.size() << "\n";
      }
    }
    else
    {
      proxyIPPort->clear_cord_transfer_plan_key();
      if (cord_trace)
        std::cout << "[CoRD] No cross-cluster transfer needed (0 plan steps)\n";
    }
    if (cord_trace)
    {
      std::cout << "[CoRD] ===== uploadCordUpdate done: stripe_id=" << stripe_id
                << " sum_update_bytes=" << sum_update_bytes
                << " clusters=" << sorted_clusters.size() << " =====\n";
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::cordPlanBeginTransfer(grpc::ServerContext *context,
                                                      const coordinator_proto::CordPlanKeyOnly *request,
                                                      coordinator_proto::RepIfSuccess *reply)
  {
    (void)context;
    reply->set_ifcommit(false);
    const std::string &pk = request->plan_key();
    if (pk.empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty plan_key");
    if (!cord_start_pending_transfer_plan(pk))
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown or already started cord plan_key");
    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::cordPlanWaitTransferComplete(grpc::ServerContext *context,
                                                               const coordinator_proto::CordPlanWaitRequest *request,
                                                               coordinator_proto::RepIfSuccess *reply)
  {
    (void)context;
    reply->set_ifcommit(false);
    reply->set_cord_xfer_timing_present(false);
    reply->set_cord_xfer_pure_sec(0.);
    reply->set_cord_xfer_grpc_sec(0.);
    const auto handler_t0 = std::chrono::steady_clock::now();
    const std::string &pk = request->plan_key();
    if (pk.empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty plan_key");
    // upload 后应已 auto-start；若 Client 仍调用 wait 而 plan 尚未启动，在此兜底启动。
    (void)cord_start_pending_transfer_plan(pk);
    std::vector<int> clusters;
    {
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      auto it = m_cord_pending_plan_clusters.find(pk);
      if (it == m_cord_pending_plan_clusters.end())
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown plan_key for wait (wrong order?)");
      clusters = it->second;
    }
    proxy_proto::CordPlanKeyMsg msg;
    msg.set_plan_key(pk);
    struct CordJoinJob {
      int cid = -1;
      bool ok = false;
      proxy_proto::SetReply rep;
      grpc::Status st;
    };
    std::vector<CordJoinJob> join_jobs;
    join_jobs.reserve(clusters.size());
    for (int cid : clusters)
    {
      if (cid < 0)
        continue;
      auto cit = m_cluster_table.find(cid);
      if (cit == m_cluster_table.end())
        continue;
      const std::string pkey = cit->second.proxy_ip + ":" + std::to_string(cit->second.proxy_port);
      auto pit = m_proxy_ptrs.find(pkey);
      if (pit == m_proxy_ptrs.end() || !pit->second)
      {
        std::cout << "[CoRD-PLAN] cordPlanWaitTransferComplete: no stub cluster=" << cid << std::endl;
        return grpc::Status(grpc::StatusCode::INTERNAL, "proxy stub missing");
      }
      CordJoinJob job;
      job.cid = cid;
      join_jobs.push_back(std::move(job));
    }

    std::vector<std::thread> join_threads;
    join_threads.reserve(join_jobs.size());
    for (size_t ji = 0; ji < join_jobs.size(); ++ji)
    {
      join_threads.emplace_back([this, &msg, &join_jobs, ji]() {
        CordJoinJob &job = join_jobs[ji];
        auto cit = m_cluster_table.find(job.cid);
        if (cit == m_cluster_table.end())
          return;
        const std::string pkey = cit->second.proxy_ip + ":" + std::to_string(cit->second.proxy_port);
        auto pit = m_proxy_ptrs.find(pkey);
        if (pit == m_proxy_ptrs.end() || !pit->second)
          return;
        grpc::ClientContext ctx;
        job.st = pit->second->cordPlanJoinExecution(&ctx, msg, &job.rep);
        job.ok = job.st.ok() && job.rep.ifcommit();
        if (!job.ok)
        {
          std::cout << "[CoRD-PLAN] cordPlanJoinExecution failed cluster=" << job.cid << " "
                    << job.st.error_message() << std::endl;
        }
      });
    }
    for (auto &th : join_threads)
      th.join();

    bool span_have = false;
    int64_t span_min_start_ms = 0;
    int64_t span_max_end_ms = 0;
    int joined_proxies = 0;
    int timing_samples = 0;
    double max_proxy_pure_xfer_sec = 0.;
    for (const auto &job : join_jobs)
    {
      if (!job.ok)
        return grpc::Status(grpc::StatusCode::INTERNAL, "cordPlanJoinExecution failed");
      ++joined_proxies;
      const proxy_proto::SetReply &rep = job.rep;
      if (rep.cord_join_xfer_timing_present())
      {
        const int64_t sm = rep.cord_join_pure_xfer_start_unix_ms();
        const int64_t em = rep.cord_join_pure_xfer_end_unix_ms();
        max_proxy_pure_xfer_sec = std::max(max_proxy_pure_xfer_sec, rep.cord_join_pure_xfer_sec());
        if (!span_have)
        {
          span_min_start_ms = sm;
          span_max_end_ms = em;
          span_have = true;
          timing_samples = 1;
        }
        else
        {
          span_min_start_ms = std::min(span_min_start_ms, sm);
          span_max_end_ms = std::max(span_max_end_ms, em);
          ++timing_samples;
        }
      }
    }
    {
      std::lock_guard<std::mutex> lk(m_cord_pending_mu);
      m_cord_pending_plan_clusters.erase(pk);
    }
    cord_clear_auto_begin_session(pk);
    const double handler_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - handler_t0).count();
    double pure_xfer_sec = 0.;
    if (span_have && span_max_end_ms >= span_min_start_ms)
    {
      pure_xfer_sec = static_cast<double>(span_max_end_ms - span_min_start_ms) / 1000.;
    }
    else if (max_proxy_pure_xfer_sec > 0.)
    {
      pure_xfer_sec = max_proxy_pure_xfer_sec;
    }
    if (pure_xfer_sec > 0.)
    {
      const double grpc_sec = std::max(0., handler_sec - pure_xfer_sec);
      reply->set_cord_xfer_timing_present(true);
      reply->set_cord_xfer_pure_sec(pure_xfer_sec);
      reply->set_cord_xfer_grpc_sec(grpc_sec);
      if (cord_trace_log(IF_DEBUG))
        std::cout << "[CoRD-PLAN][Coordinator] xfer_wait breakdown plan_key=" << pk
                  << " handler_sec=" << handler_sec << " pure_xfer_sec=" << pure_xfer_sec
                  << " grpc_sec=" << grpc_sec << " joined_proxies=" << joined_proxies
                  << " timing_samples=" << timing_samples << '\n';
    }
    else if (joined_proxies > 0)
    {
      reply->set_cord_xfer_timing_present(true);
      reply->set_cord_xfer_pure_sec(0.);
      reply->set_cord_xfer_grpc_sec(handler_sec);
      if (cord_trace_log(IF_DEBUG))
        std::cout << "[CoRD-PLAN][Coordinator] xfer_wait breakdown plan_key=" << pk
                  << " handler_sec=" << handler_sec << " pure_xfer_sec=n/a grpc_sec=" << handler_sec
                  << '\n';
    }
    (void)timing_samples;
    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }

  void CoordinatorImpl::notify_proxy_cord_local_parity_bundle(int target_cluster_id,
                                                              const proxy_proto::CordLocalParityBundle &bundle)
  {
    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    if (m_cluster_table.find(target_cluster_id) == m_cluster_table.end())
    {
      std::cout << "[CoRD-LP] invalid target cluster " << target_cluster_id << std::endl;
      return;
    }
    std::string chosen_proxy =
        m_cluster_table[target_cluster_id].proxy_ip + ":" +
        std::to_string(m_cluster_table[target_cluster_id].proxy_port);
    auto pit = m_proxy_ptrs.find(chosen_proxy);
    if (pit == m_proxy_ptrs.end())
    {
      std::cout << "[CoRD-LP] no proxy stub for " << chosen_proxy << std::endl;
      return;
    }
    grpc::Status status = pit->second->scheduleCordLocalParityApply(&cont, bundle, &set_reply);
    if (!status.ok())
      std::cout << "[CoRD-LP] scheduleCordLocalParityApply key=" << bundle.key()
                << " failed: " << status.error_message() << std::endl;
  }

  grpc::Status CoordinatorImpl::uploadCordLocalParityApply(
      grpc::ServerContext *context,
      const coordinator_proto::CordUpdateRequest *request,
      coordinator_proto::RepIfSuccess *reply)
  {
    (void)context;
    reply->set_ifcommit(false);
    const int stripe_id = request->stripe_id();
    if (m_stripe_table.find(stripe_id) == m_stripe_table.end())
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "stripe_id not found");
    Stripe *stripe = &m_stripe_table[stripe_id];
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int k = stripe->k;

    if (request->interval_count() > 0 &&
        request->interval_count() != request->update_intervals_size())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "interval_count does not match update_intervals size");
    if (request->update_intervals_size() == 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty update_intervals");

    std::map<int, std::vector<std::pair<int, int>>> block_intervals;
    for (int ri = 0; ri < request->update_intervals_size(); ++ri)
    {
      const auto &r = request->update_intervals(ri);
      const int s = r.logical_offset_start();
      const int e = r.logical_offset_end();
      if (e <= s)
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid half-open interval [start,end)");
      const int stripe_data_bytes = k * block_size;
      if (s < 0 || s > stripe_data_bytes || e > stripe_data_bytes)
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "logical interval out of stripe data range");
      cord_add_logical_range_to_data_blocks(block_size, k, s, e, &block_intervals);
    }

    std::map<int, std::vector<CordSliceRec>> cluster_slices;
    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      Block *bp = stripe->blocks[bid];
      for (const auto &seg : kv.second)
      {
        CordSliceRec rec;
        rec.block_id = bid;
        rec.block_offset = seg.first;
        rec.len = seg.second - seg.first;
        if (rec.len <= 0)
          continue;
        cluster_slices[bp->map2cluster].push_back(rec);
      }
    }
    for (auto &cs : cluster_slices)
    {
      std::sort(cs.second.begin(), cs.second.end(),
                [](const CordSliceRec &a, const CordSliceRec &b)
                {
                  if (a.block_id != b.block_id)
                    return a.block_id < b.block_id;
                  return a.block_offset < b.block_offset;
                });
    }

    if (cluster_slices.empty())
    {
      reply->set_ifcommit(true);
      return grpc::Status::OK;
    }

    std::map<std::tuple<int, int, int>, LpWorkAgg> agg;
    cord_alg2::TransferParams tp;

    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      const int gnum = stripe->blocks[bid]->map2group;
      const int lb = cord_lp_find_local_parity_block(*stripe, gnum);
      if (lb < 0)
        continue;

      std::vector<int> dblocks;
      for (const auto &kv2 : block_intervals)
      {
        if (kv2.first >= 0 && kv2.first < k && stripe->blocks[kv2.first]->map2group == gnum)
          dblocks.push_back(kv2.first);
      }
      std::sort(dblocks.begin(), dblocks.end());
      dblocks.erase(std::unique(dblocks.begin(), dblocks.end()), dblocks.end());
      const int hub_blk = cord_lp_pick_hub_global(*stripe, block_intervals, dblocks, tp);
      const int hub_c = stripe->blocks[hub_blk]->map2cluster;
      (void)hub_c;

      for (const auto &seg : kv.second)
      {
        const int a = seg.first;
        const int b = seg.second;
        const int seg_len = b - a;
        if (seg_len <= 0)
          continue;
        const int lo = bid * block_size + a;
        std::map<int, std::pair<int, int>> b2s;
        int ps = 0, po = 0;
        bool merge = false;
        std::string err;
        if (!build_slice_plan_for_logical_range(stripe, lo, seg_len, &b2s, &ps, &po, &merge, &err))
        {
          std::cout << "[CoRD-LP] build_slice failed: " << err << std::endl;
          continue;
        }
        const auto pit = b2s.find(lb);
        if (pit == b2s.end())
          continue;
        const int psz = pit->second.first;
        const int poff = pit->second.second;

        const int cid = stripe->blocks[bid]->map2cluster;
        const auto csit = cluster_slices.find(cid);
        if (csit == cluster_slices.end())
          continue;
        uint64_t blob_off = 0;
        if (!cord_lp_find_delta_blob_offset(csit->second, bid, a, seg_len, &blob_off))
        {
          std::cout << "[CoRD-LP] missing delta layout for stripe=" << stripe_id << " cluster=" << cid
                    << " block=" << bid << std::endl;
          continue;
        }
        const int delta_node_id = m_cluster_table[cid].nodes.front();
        const Node &delta_node = m_node_table[delta_node_id];
        const std::string cord_key = m_toolbox->gen_cord_key(stripe_id, cid);
        const std::string blob_key = cord_key + "_delta";

        const auto lk = std::make_tuple(lb, poff, psz);
        LpWorkAgg &w = agg[lk];
        if (w.local_block_id < 0)
        {
          w.local_block_id = lb;
          w.local_block_key = stripe->blocks[lb]->block_key;
          const Node &ln = m_node_table[stripe->blocks[lb]->map2node];
          w.local_dn_ip = ln.node_ip;
          w.local_dn_port = ln.node_port;
          w.parity_slice_offset = poff;
          w.parity_slice_size = psz;
        }
        LpFetchSpec fs;
        fs.blob_key = blob_key;
        fs.dn_ip = delta_node.node_ip;
        fs.dn_port = delta_node.node_port;
        fs.blob_off = blob_off;
        fs.read_len = static_cast<uint64_t>(seg_len);
        fs.acc_offset = 0;
        fs.data_block_id = bid;
        fs.source_cluster_id = cid;
        w.fetches.push_back(std::move(fs));

        // std::cout << "[CoRD-LP] map2group=" << gnum << " hub_global_blk=" << hub_blk
        //           << " data_blk=" << bid << " seg=[" << a << "," << b << ") -> LP blk " << lb
        //           << " parity_off=" << poff << " len=" << psz << " fetch " << blob_key << "@" << blob_off
        //           << " len=" << seg_len << std::endl;
      }
    }

    if (agg.empty())
    {
      reply->set_ifcommit(true);
      return grpc::Status::OK;
    }

    std::map<int, proxy_proto::CordLocalParityBundle> by_cluster;
    for (auto &kv : agg)
    {
      LpWorkAgg &w = kv.second;
      const int target_c = stripe->blocks[w.local_block_id]->map2cluster;
      proxy_proto::CordLocalParityBundle &bd = by_cluster[target_c];
      if (bd.key().empty())
        bd.set_key(m_toolbox->gen_cord_key(stripe_id, target_c) + "_lp");
      bd.set_stripe_id(stripe_id);
      auto *itm = bd.add_items();
      itm->set_local_block_id(w.local_block_id);
      itm->set_local_block_key(w.local_block_key);
      itm->set_local_datanode_ip(w.local_dn_ip);
      itm->set_local_datanode_port(w.local_dn_port);
      itm->set_parity_slice_offset(w.parity_slice_offset);
      itm->set_parity_slice_size(w.parity_slice_size);
      for (const auto &fs : w.fetches)
      {
        auto *f = itm->add_fetches();
        f->set_blob_key(fs.blob_key);
        f->set_datanode_ip(fs.dn_ip);
        f->set_datanode_port(fs.dn_port);
        f->set_blob_offset(fs.blob_off);
        f->set_read_len(fs.read_len);
        f->set_acc_offset(fs.acc_offset);
      }
    }

    for (auto &bc : by_cluster)
    {
      std::thread th(&CoordinatorImpl::notify_proxy_cord_local_parity_bundle, this, bc.first, bc.second);
      th.join();
    }

    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }


  grpc::Status CoordinatorImpl::uploadCordLocalParityViaGlobalHub(
      grpc::ServerContext *context,
      const coordinator_proto::CordUpdateRequest *request,
      coordinator_proto::RepIfSuccess *reply)
  {
    (void)context;
    reply->set_ifcommit(false);
    const int stripe_id = request->stripe_id();
    if (m_stripe_table.find(stripe_id) == m_stripe_table.end())
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "stripe_id not found");
    Stripe *stripe = &m_stripe_table[stripe_id];
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int k = stripe->k;

    if (request->interval_count() > 0 &&
        request->interval_count() != request->update_intervals_size())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "interval_count does not match update_intervals size");
    if (request->update_intervals_size() == 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty update_intervals");

    std::map<int, std::vector<std::pair<int, int>>> block_intervals;
    for (int ri = 0; ri < request->update_intervals_size(); ++ri)
    {
      const auto &r = request->update_intervals(ri);
      const int s = r.logical_offset_start();
      const int e = r.logical_offset_end();
      if (e <= s)
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid half-open interval [start,end)");
      const int stripe_data_bytes = k * block_size;
      if (s < 0 || s > stripe_data_bytes || e > stripe_data_bytes)
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "logical interval out of stripe data range");
      cord_add_logical_range_to_data_blocks(block_size, k, s, e, &block_intervals);
    }

    std::map<int, std::vector<CordSliceRec>> cluster_slices;
    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      Block *bp = stripe->blocks[bid];
      for (const auto &seg : kv.second)
      {
        CordSliceRec rec;
        rec.block_id = bid;
        rec.block_offset = seg.first;
        rec.len = seg.second - seg.first;
        if (rec.len <= 0)
          continue;
        cluster_slices[bp->map2cluster].push_back(rec);
      }
    }
    for (auto &cs : cluster_slices)
    {
      std::sort(cs.second.begin(), cs.second.end(),
                [](const CordSliceRec &a, const CordSliceRec &b)
                {
                  if (a.block_id != b.block_id)
                    return a.block_id < b.block_id;
                  return a.block_offset < b.block_offset;
                });
    }

    if (cluster_slices.empty())
    {
      reply->set_ifcommit(true);
      return grpc::Status::OK;
    }

    std::map<std::tuple<int, int, int>, LpWorkAgg> agg;
    cord_alg2::TransferParams tp;

    for (const auto &kv : block_intervals)
    {
      const int bid = kv.first;
      if (bid < 0 || bid >= k)
        continue;
      const int gnum = stripe->blocks[bid]->map2group;
      const int lb = cord_lp_find_local_parity_block(*stripe, gnum);
      if (lb < 0)
        continue;

      std::vector<int> dblocks;
      for (const auto &kv2 : block_intervals)
      {
        if (kv2.first >= 0 && kv2.first < k && stripe->blocks[kv2.first]->map2group == gnum)
          dblocks.push_back(kv2.first);
      }
      std::sort(dblocks.begin(), dblocks.end());
      dblocks.erase(std::unique(dblocks.begin(), dblocks.end()), dblocks.end());
      const int hub_blk = cord_lp_pick_hub_global(*stripe, block_intervals, dblocks, tp);
      const int hub_c = stripe->blocks[hub_blk]->map2cluster;
      (void)hub_c;

      for (const auto &seg : kv.second)
      {
        const int a = seg.first;
        const int b = seg.second;
        const int seg_len = b - a;
        if (seg_len <= 0)
          continue;
        const int lo = bid * block_size + a;
        std::map<int, std::pair<int, int>> b2s;
        int ps = 0, po = 0;
        bool merge = false;
        std::string err;
        if (!build_slice_plan_for_logical_range(stripe, lo, seg_len, &b2s, &ps, &po, &merge, &err))
        {
          std::cout << "[CoRD-LP-GH] build_slice failed: " << err << std::endl;
          continue;
        }
        const auto pit = b2s.find(lb);
        if (pit == b2s.end())
          continue;
        const int psz = pit->second.first;
        const int poff = pit->second.second;

        const int cid = stripe->blocks[bid]->map2cluster;
        const auto csit = cluster_slices.find(cid);
        if (csit == cluster_slices.end())
          continue;
        uint64_t blob_off = 0;
        if (!cord_lp_find_delta_blob_offset(csit->second, bid, a, seg_len, &blob_off))
        {
          std::cout << "[CoRD-LP-GH] missing delta layout for stripe=" << stripe_id << " cluster=" << cid
                    << " block=" << bid << std::endl;
          continue;
        }
        const int delta_node_id = m_cluster_table[cid].nodes.front();
        const Node &delta_node = m_node_table[delta_node_id];
        const std::string cord_key = m_toolbox->gen_cord_key(stripe_id, cid);
        const std::string blob_key = cord_key + "_delta";

        const auto lk = std::make_tuple(lb, poff, psz);
        LpWorkAgg &w = agg[lk];
        if (w.local_block_id < 0)
        {
          w.local_block_id = lb;
          w.local_block_key = stripe->blocks[lb]->block_key;
          const Node &ln = m_node_table[stripe->blocks[lb]->map2node];
          w.local_dn_ip = ln.node_ip;
          w.local_dn_port = ln.node_port;
          w.parity_slice_offset = poff;
          w.parity_slice_size = psz;
        }
        LpFetchSpec fs;
        fs.blob_key = blob_key;
        fs.dn_ip = delta_node.node_ip;
        fs.dn_port = delta_node.node_port;
        fs.blob_off = blob_off;
        fs.read_len = static_cast<uint64_t>(seg_len);
        fs.acc_offset = 0;
        fs.data_block_id = bid;
        fs.source_cluster_id = cid;
        w.fetches.push_back(std::move(fs));
      }
    }

    if (agg.empty())
    {
      reply->set_ifcommit(true);
      return grpc::Status::OK;
    }

    if (!run_cord_lp_global_hub_aggregation(m_cluster_table, m_proxy_ptrs, stripe_id, stripe, block_intervals, agg))
    {
      reply->set_ifcommit(false);
      return grpc::Status(grpc::StatusCode::INTERNAL, "CoRD-LP global hub aggregation failed");
    }
    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_add_plans(Stripe *stripe)
  {
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      int mapped_cluster_id = stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;
      size_t append_size = stripe->group_to_blocks[i].size() * m_sys_config->BlockSize;

      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(append_size);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(mapped_cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);

      for (int j = 0; j < stripe->group_to_blocks[i].size(); j++)
      {
        addBlockToAppendPlan(plan, stripe->blocks[stripe->group_to_blocks[i][j]], m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node], std::make_pair(m_sys_config->BlockSize, 0));
      }

      add_plans.push_back(plan);
    }

    return add_plans;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_sub_add_plans(Stripe *stripe, size_t subset_size)
  {
    int data_block_num = subset_size / m_sys_config->BlockSize;
    int k = m_sys_config->k;
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      int block_num = 0;
      for (int j = 0; j < stripe->group_to_blocks[i].size(); j++)
      {
        int block_id = stripe->group_to_blocks[i][j];
        if(block_id < k && block_id >= data_block_num)
        {
          continue;
        }
        addBlockToAppendPlan(plan, stripe->blocks[stripe->group_to_blocks[i][j]], m_node_table[stripe->blocks[stripe->group_to_blocks[i][j]]->map2node], std::make_pair(m_sys_config->BlockSize, 0));
        block_num++;
      }

      size_t append_size = block_num * m_sys_config->BlockSize;
      if(append_size == 0)
      {
        //plan.set_append_size(0);
        //add_plans.push_back(plan);
        continue; // no data to append
      }

      int mapped_cluster_id = stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster;

      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(mapped_cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);
      plan.set_append_size(append_size);

      add_plans.push_back(plan);
    }

    return add_plans;
  }

  void CoordinatorImpl::print_stripe_data_placement(Stripe &stripe)
  {
    std::cout << "Stripe " << stripe.stripe_id << " data placement: " << std::endl;
    for (int i = 0; i < stripe.num_groups; i++)
    {
      const std::vector<int> &group_block_ids = stripe.group_to_blocks[i];
      if (group_block_ids.empty())
      {
        std::cout << "Group " << i << ": (0 blocks)" << std::endl;
        continue;
      }
      // 不再把 block_id 当作 stripe.blocks 的下标，统一按 block_id 查对象。
      const Block *first_block = nullptr;
      for (const auto *blk : stripe.blocks)
      {
        if (blk->block_id == group_block_ids[0])
        {
          first_block = blk;
          break;
        }
      }
      int mapped_cluster = (first_block == nullptr) ? -1 : first_block->map2cluster;
      std::cout << "Group " << i << ": (" << group_block_ids.size() << " blocks, mapped to cluster " << mapped_cluster << ") ";
      for (int block_id : group_block_ids)
      {
        const Block *target_block = nullptr;
        for (const auto *blk : stripe.blocks)
        {
          if (blk->block_id == block_id)
          {
            target_block = blk;
            break;
          }
        }
        if (target_block != nullptr)
        {
          std::cout << target_block->block_key << "(c" << target_block->map2cluster << ") ";
        }
        else
        {
          std::cout << "[missing_block_id_" << block_id << "] ";
        }
      }
      std::cout << std::endl;
    }
  }

  // set only the full block stripe
  grpc::Status CoordinatorImpl::uploadSetValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    size_t setSizeBytes = keyValueSize->valuesizebytes();
    std::string code_type = m_sys_config->CodeType;
    assert(setSizeBytes == static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->k) && "set size is not equal to the block stripe size!");
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "RandomLRC" || code_type == "SplitParityLRC" || code_type == "CordXueLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, RandomLRC, SplitParityLRC, or CordXueLRC!");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
    t_stripe.g_m = m_sys_config->r;
    t_stripe.l = m_sys_config->z;
    t_stripe.object_keys.push_back(clientID);
    if (code_type == "UniLRC" || code_type == "AzureLRC")
    {
      initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "OptimalLRC")
    {
      initialize_optimal_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "UniformLRC")
    {
      initialize_uniform_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "RandomLRC")
    {
      initialize_random_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "SplitParityLRC")
    {
      initialize_split_parity_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "CordXueLRC")
    {
      initialize_cord_xue_lrc_stripe_placement(&t_stripe);
    }
    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_add_plans(&t_stripe);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    size_t sum_append_size = 0;

    // 并行通知所有 proxy，减少串行 gRPC 延迟
    {
      const int plan_count = static_cast<int>(add_plans.size());
      std::vector<std::thread> notify_threads;
      notify_threads.reserve(static_cast<size_t>(plan_count));
      for (int i = 0; i < plan_count; ++i)
      {
        notify_threads.emplace_back([this, &plan = add_plans[static_cast<size_t>(i)]]() {
          notify_proxies_ready(plan);
        });
      }
      for (auto &t : notify_threads)
        t.join();
    }

    // 串行填充 proxyIPPort（protobuf 非线程安全）
    for (const auto &plan : add_plans)
    {
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::SET_XFER_PORT_OFFSET); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadSubsetValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    std::string clientID = keyValueSize->key();
    size_t setSizeBytes = keyValueSize->valuesizebytes();
    std::string code_type = m_sys_config->CodeType;
    assert(setSizeBytes <= static_cast<size_t>(m_sys_config->BlockSize) * static_cast<size_t>(m_sys_config->k) && "subset size is larger than the block size!");
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "RandomLRC" || code_type == "SplitParityLRC" || code_type == "CordXueLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, RandomLRC, SplitParityLRC, or CordXueLRC!");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
    t_stripe.g_m = m_sys_config->r;
    t_stripe.l = m_sys_config->z;
    t_stripe.object_keys.push_back(clientID);
    if (code_type == "UniLRC" || code_type == "AzureLRC")
    {
      initialize_unilrc_and_azurelrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "OptimalLRC")
    {
      initialize_optimal_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "UniformLRC")
    {
      initialize_uniform_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "RandomLRC")
    {
      initialize_random_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "SplitParityLRC")
    {
      initialize_split_parity_lrc_stripe_placement(&t_stripe);
    }
    else if (code_type == "CordXueLRC")
    {
      initialize_cord_xue_lrc_stripe_placement(&t_stripe);
    }
    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_sub_add_plans(&t_stripe, setSizeBytes);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    size_t sum_append_size = 0;

    // 并行通知所有 proxy，减少串行 gRPC 延迟
    {
      const int plan_count = static_cast<int>(add_plans.size());
      std::vector<std::thread> notify_threads;
      notify_threads.reserve(static_cast<size_t>(plan_count));
      for (int i = 0; i < plan_count; ++i)
      {
        notify_threads.emplace_back([this, &plan = add_plans[static_cast<size_t>(i)]]() {
          notify_proxies_ready(plan);
        });
      }
      for (auto &t : notify_threads)
        t.join();
    }

    // 串行填充 proxyIPPort（protobuf 非线程安全）
    for (const auto &plan : add_plans)
    {
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::SET_XFER_PORT_OFFSET); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      //proxyIPPort->add_group_ids(group_id);
      sum_append_size += plan.append_size();
      //group_id++;
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

    m_stripe_table[t_stripe.stripe_id] = std::move(t_stripe);

    return grpc::Status::OK;
  }
  
  std::vector<int> CoordinatorImpl::get_recovery_group_ids(std::string code_type, int k, int r, int z, int failed_block_id)
  {
    std::vector<int> recovery_group_ids;
    if (is_azure_like_code(code_type))
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        // global parity repair needs all local groups (0 .. z-1); dest rack is global group z
        for (int i = 0; i < z; i++)
        {
          recovery_group_ids.push_back(i);
        }
      }
      else if (failed_block_id >= k + r)
      {
        recovery_group_ids.push_back(failed_block_id - k - r);
      }
      else
      {
        recovery_group_ids.push_back(failed_block_id / (k / z));
      }
    }
    else if (code_type == "UniLRC")
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        recovery_group_ids.push_back((failed_block_id - k) / (r / z));
      }
      else if (failed_block_id >= k + r)
      {
        recovery_group_ids.push_back(failed_block_id - k - r);
      }
      else
      {
        recovery_group_ids.push_back(failed_block_id / (k / z));
      }
    }
    else if (code_type == "OptimalLRC")
    {
      if (failed_block_id >= k && failed_block_id < k + r)
      {
        int group_num = (k / z / (r + 1) + (bool)(k / z % (r + 1))) * z + 1;
        recovery_group_ids.push_back(group_num - 1);
        for (int i = 0; i < group_num / z; i++)
        {
          recovery_group_ids.push_back(i);
        }
      }
      else if (failed_block_id >= k + r)
      {
        int local_group_size = k / z;
        int local_group_id = (failed_block_id - k - r);
        int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
        int group_num = z * group_num_of_one_local_group + 1;
        recovery_group_ids.push_back((local_group_id + 1) * group_num_of_one_local_group - 1);
        for (int i = local_group_id * group_num_of_one_local_group; i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
        {
          recovery_group_ids.push_back(i);
        }
        recovery_group_ids.push_back(group_num - 1);
      }
      else
      {
        int local_group_size = k / z;
        int group_num_of_one_local_group = local_group_size / (r + 1) + 1;
        int local_group_id = failed_block_id / local_group_size;
        int group_id_in_local_group = failed_block_id % local_group_size / (r + 1);
        recovery_group_ids.push_back(local_group_id * group_num_of_one_local_group + group_id_in_local_group);
        for (int i = 0; i < group_num_of_one_local_group; i++)
        {
          if (i != group_id_in_local_group)
          {
            recovery_group_ids.push_back(local_group_id * group_num_of_one_local_group + i);
          }
        }
        int group_num = z * group_num_of_one_local_group + 1;
        recovery_group_ids.push_back(group_num - 1);
      }
    }
    else if (code_type == "UniformLRC")
    {
      if (failed_block_id >= k + r)
      {
        int larger_local_group_num = (k + r) % z;
        int local_group_id = failed_block_id - k - r;
        int local_group_size = (k + r) / z;
        int group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
        if (local_group_id + larger_local_group_num < z)
        {
          recovery_group_ids.push_back((local_group_id + 1) * group_num_of_one_local_group - 1);
          for (int i = local_group_id * group_num_of_one_local_group; i < (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
          {
            recovery_group_ids.push_back(i);
          }
        }
        else
        {
          int smaller_local_group_num = z - larger_local_group_num;
          int group_num_of_all_small_group = smaller_local_group_num * group_num_of_one_local_group;
          local_group_size++;
          group_num_of_one_local_group = local_group_size / r + (bool)(local_group_size % r);
          local_group_id = local_group_id - smaller_local_group_num;
          recovery_group_ids.push_back(group_num_of_all_small_group + (local_group_id + 1) * group_num_of_one_local_group - 1);
          for (int i = group_num_of_all_small_group + local_group_id * group_num_of_one_local_group; i < group_num_of_all_small_group + (local_group_id + 1) * group_num_of_one_local_group - 1; i++)
          {
            recovery_group_ids.push_back(i);
          }
        }
      }
      else if (failed_block_id < k + r)
      {
        int larger_local_group_num = (k + r) % z;
        int smaller_local_group_num = z - larger_local_group_num;
        int local_group_size = (k + r) / z;
        int group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
        int block_num_of_smaller_local_group = (z - larger_local_group_num) * local_group_size;
        int group_num_of_smaller_local_group = smaller_local_group_num * group_num_of_one_local_group;
        int local_group_id = 0;
        if (failed_block_id < block_num_of_smaller_local_group)
        {
          local_group_id = failed_block_id / local_group_size;
          int block_num_in_previous_local_group = local_group_id * local_group_size;
          int group_id = local_group_id * group_num_of_one_local_group + (failed_block_id - block_num_in_previous_local_group) / r;
          recovery_group_ids.push_back(group_id);
          for (int i = local_group_id * group_num_of_one_local_group; i < local_group_id * group_num_of_one_local_group + group_num_of_one_local_group; i++)
          {
            if (i != group_id)
            {
              recovery_group_ids.push_back(i);
            }
          }
        }
        else
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / r + bool(local_group_size % r);
          local_group_id = (failed_block_id - block_num_of_smaller_local_group) / local_group_size;
          int block_num_in_previous_local_group = local_group_id * local_group_size + block_num_of_smaller_local_group;
          int group_id = local_group_id * group_num_of_one_local_group + (failed_block_id - block_num_in_previous_local_group) / r;
          recovery_group_ids.push_back(group_id + group_num_of_smaller_local_group);
          for (int i = local_group_id * group_num_of_one_local_group; i < local_group_id * group_num_of_one_local_group + group_num_of_one_local_group; i++)
          {
            if (i != group_id)
            {
              recovery_group_ids.push_back(i + group_num_of_smaller_local_group);
            }
          }
        }
      }
    }

    return recovery_group_ids;
  }

  void CoordinatorImpl::init_recovery_group_lookup_table()
  {
    for (int i = 0; i < m_sys_config->n; i++)
    {
      m_recovery_group_lookup_table[i] = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, i);
    }
  }

  std::vector<int> CoordinatorImpl::get_data_block_num_per_group(int k, int r, int z, std::string code_type)
  {
    std::vector<int> data_block_num_per_group;
    if (is_azure_like_code(code_type))
    {
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back((k / z));
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "OptimalLRC")
    {
      int group_size = r + 1;
      int local_group_size = (k / z);
      int group_num_of_one_local_group = local_group_size / group_size + 1;
      int group_num = z * group_num_of_one_local_group + 1;
      for (int i = 0; i < group_num - 1; i++)
      {
        if ((i + 1) % group_num_of_one_local_group)
        {
          data_block_num_per_group.push_back(group_size);
        }
        else
        {
          data_block_num_per_group.push_back(local_group_size % group_size);
        }
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniformLRC")
    {
      /*int group_size = r + 1;
      int local_group_size = int((k + r) / z);
      int larger_local_group_num = int((k + r) % z);

      int group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
      for (int i = 0; i < z - 1; i++)
      {
        if (i + larger_local_group_num == z)
        {
          local_group_size++;
          group_num_of_one_local_group = local_group_size / group_size + (bool)(local_group_size % group_size);
        }
        for (int j = 0; j < group_num_of_one_local_group; j++)
        {
          if (j == group_num_of_one_local_group - 1)
          {
            data_block_num_per_group.push_back(local_group_size % group_size);
          }
          else
          {
            data_block_num_per_group.push_back(group_size);
          }
        }
      }
      data_block_num_per_group.push_back(local_group_size - r);
      for(int i = 0; i < group_num_of_one_local_group -1; i++)
      {
        data_block_num_per_group.push_back(0);
      }*/

      for(int i = 0; i < z -1; i++){
        data_block_num_per_group.push_back((k+r) / z);
      }
      data_block_num_per_group.push_back(0);
    }
    else if (code_type == "UniLRC")
    {
      int local_data_num = k / z;
      for (int i = 0; i < z; i++)
      {
        data_block_num_per_group.push_back(local_data_num);
      }
    }
    return data_block_num_per_group;
  }

  
  void CoordinatorImpl::getStripeFromProxy(std::string client_ip, int client_port, std::string proxy_ip, int proxy_port, int stripe_id, int group_id, std::vector<int> block_ids)
  {
    std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << std::endl;
    for(int i = 0; i < block_ids.size(); i++){
      std::cout << "block_id: " << block_ids[i] << std::endl;
    }
    grpc::ClientContext cont;
    proxy_proto::StripeAndBlockIDs stripe_block_ids;
    proxy_proto::GetReply stripe_reply;
    stripe_block_ids.set_stripe_id(stripe_id);
    stripe_block_ids.set_clientip(client_ip);
    stripe_block_ids.set_clientport(client_port);
    stripe_block_ids.set_group_id(group_id);

    for (int i = 0; i < block_ids.size(); i++)
    {
      stripe_block_ids.add_block_ids(block_ids[i]);
      stripe_block_ids.add_block_keys(m_stripe_table[stripe_id].blocks[block_ids[i]]->block_key);
      stripe_block_ids.add_datanodeips(m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node].node_ip);
      stripe_block_ids.add_datanodeports(m_node_table[m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node].node_port);
    }
    grpc::Status status = m_proxy_ptrs[proxy_ip + ":" + std::to_string(proxy_port)]->getBlocks(&cont, stripe_block_ids, &stripe_reply);
    if (status.ok())
    {
      std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << " succeeded!" << std::endl;
    }
    else
    {
      std::cout << "[GET] getting stripe " << stripe_id << " from proxy " << proxy_ip << ":" << proxy_port << " failed!" << std::endl;
    }
  }


  grpc::Status 
  CoordinatorImpl::getStripe(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {

    //std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    int stripe_id = std::stoi(keyClient->key());
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k = t_stripe.k;
    int num_data_groups = t_stripe.num_groups;
    std::string code_type = m_sys_config->CodeType;
    if(code_type != "UniLRC"){
      num_data_groups--;
    }
    //std::cout << "[GET] getting stripe " << stripe_id << " with " << num_data_groups << " data groups" << std::endl;
    std::vector<int> block_num_per_group = get_data_block_num_per_group(k, m_sys_config->r, m_sys_config->z, code_type);
    std::vector<int> get_cluster_ids;
    for (int i = 0; i < num_data_groups; i++)
    {
      get_cluster_ids.push_back(t_stripe.blocks[t_stripe.group_to_blocks[i][0]]->map2cluster);
      //std::cout << "group " << i << " is mapped to cluster " << get_cluster_ids[i] << std::endl;
    }
    for (int i = 0; i < num_data_groups; i++)
    {
      proxyIPPort->add_proxyips(m_cluster_table[get_cluster_ids[i]].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[get_cluster_ids[i]].proxy_port);
      proxyIPPort->add_cluster_slice_sizes(block_num_per_group[i]);
    }
    /*for(int i = 0; i < t_stripe.num_groups; i++){
      m_proxy_ptrs[proxyIPPort->proxyips(i) + ":" + std::to_string(proxyIPPort->proxyports(i))]->getStripe(stripe_id, t_stripe.group_to_blocks[i]);
    }*/
    std::vector<std::thread> threads;
    for (int i = 0; i < num_data_groups; i++)
    {
      std::vector<int> block_ids;
      for (int j = 0; j < t_stripe.group_to_blocks[i].size(); j++)
      {
        if(t_stripe.blocks[t_stripe.group_to_blocks[i][j]]->block_id < k){
          block_ids.push_back(t_stripe.group_to_blocks[i][j]);
        }
      }
      threads.push_back(std::thread(&CoordinatorImpl::getStripeFromProxy, this, keyClient->clientip(), keyClient->clientport(), 
        proxyIPPort->proxyips(i), proxyIPPort->proxyports(i), stripe_id, i, block_ids));
    }
    for (auto &thread : threads)
    {
      thread.detach();
    }
    /*std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << "[GET] getting stripe " << stripe_id << " took " << duration.count() << " seconds" << std::endl;*/

    return grpc::Status::OK;
  }
  
  grpc::Status
  CoordinatorImpl::getBlocks(
      grpc::ServerContext *context,
      const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort
  )
  {
    std::string client_ip = blockIDsClient->clientip();
    int client_port = blockIDsClient->clientport();
    int start_block_id = blockIDsClient->start_block_id();
    int end_block_id = blockIDsClient->end_block_id();
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    std::vector<int> relative_block_ids;
    for(int i = start_block_id; i <= end_block_id; i++){
      int stripe_id = i / m_sys_config->k;
      stripe_ids.push_back(stripe_id);
      block_ids.push_back(i % m_sys_config->k);
      relative_block_ids.push_back(i - start_block_id);
    }
    std::vector<int> get_cluster_ids;
    std::vector<int> unique_cluster_ids;
    for (int i = 0; i < stripe_ids.size(); i++)
    {
      get_cluster_ids.push_back(m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2cluster);
      if(std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(), get_cluster_ids[i]) == unique_cluster_ids.end()){
        unique_cluster_ids.push_back(get_cluster_ids[i]);
      }
    }
    proxy_proto::StripeAndBlockIDs stripe_block_ids[unique_cluster_ids.size()];
    for(int i = 0; i < stripe_ids.size(); i++){
      int idx = std::find(unique_cluster_ids.begin(), unique_cluster_ids.end(), get_cluster_ids[i]) - unique_cluster_ids.begin();
      stripe_block_ids[idx].add_block_ids(relative_block_ids[i]);
      stripe_block_ids[idx].add_block_keys(m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->block_key);
      stripe_block_ids[idx].add_datanodeips(m_node_table[m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node].node_ip);
      stripe_block_ids[idx].add_datanodeports(m_node_table[m_stripe_table[stripe_ids[i]].blocks[block_ids[i]]->map2node].node_port);
    }
    std::vector<std::thread> get_threads;
    for(int i = 0; i < unique_cluster_ids.size(); i++){
      get_threads.push_back(std::thread([this, &stripe_block_ids, &client_ip, &client_port, &unique_cluster_ids, i](){
        grpc::ClientContext cont;
        proxy_proto::GetReply stripe_reply;
        stripe_block_ids[i].set_clientip(client_ip);
        stripe_block_ids[i].set_clientport(client_port);
        grpc::Status status = m_proxy_ptrs[m_cluster_table[unique_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[unique_cluster_ids[i]].proxy_port)]->getBlocks(&cont, stripe_block_ids[i], &stripe_reply);
        if (status.ok())
        {
          std::cout << "[GET] getting blocks from proxy " << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":" << m_cluster_table[unique_cluster_ids[i]].proxy_port << " succeeded!" << std::endl;
        }
        else
        {
          std::cout << "[GET] getting blocks from proxy " << m_cluster_table[unique_cluster_ids[i]].proxy_ip << ":" << m_cluster_table[unique_cluster_ids[i]].proxy_port << " failed!" << std::endl;
        }
      }));
    }
    for (auto &thread : get_threads)
    {
      thread.join();
    }
    return grpc::Status::OK;

  }

  grpc::Status
  CoordinatorImpl::getDegradedReadBlocks(
      grpc::ServerContext *context,
      const coordinator_proto::BlockIDsAndClientIP *blockIDsClient,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort
  )
  {
    std::string client_ip = blockIDsClient->clientip();
    int client_port = blockIDsClient->clientport();
    int start_block_id = blockIDsClient->start_block_id();
    int end_block_id = blockIDsClient->end_block_id();
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    std::vector<int> relative_block_ids;
    for(int i = start_block_id; i <= end_block_id; i++){
      int stripe_id = i / m_sys_config->k;
      stripe_ids.push_back(stripe_id);
      block_ids.push_back(i % m_sys_config->k);
      relative_block_ids.push_back(i - start_block_id);
    }
    for(int i = 0; i < stripe_ids.size(); i++){
      degraded_read_one_block_for_workload(stripe_ids[i], block_ids[i], client_ip, client_port, relative_block_ids[i]);
    }
    return grpc::Status::OK;

  }


  grpc::Status
  CoordinatorImpl::getValue(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RepIfGetSuccess *getReplyClient)
  {
    try
    {
      std::string key = keyClient->key();
      std::string client_ip = keyClient->clientip();
      int client_port = keyClient->clientport();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_mutex.unlock();
      int k = m_encode_parameters.k_datablock;
      int g_m = m_encode_parameters.g_m_globalparityblock;
      int l = m_encode_parameters.l_localparityblock;
      // int b = m_encode_parameters.b_datapergroup;

      grpc::ClientContext decode_and_get;
      proxy_proto::ObjectAndPlacement object_placement;
      grpc::Status status;
      proxy_proto::GetReply get_reply;
      getReplyClient->set_valuesizebytes(object_info.object_size);
      object_placement.set_key(key);
      object_placement.set_valuesizebyte(object_info.object_size);
      object_placement.set_k(k);
      object_placement.set_l(l);
      object_placement.set_g_m(g_m);
      object_placement.set_stripe_id(object_info.map2stripe);
      object_placement.set_encode_type(m_encode_parameters.encodetype);
      object_placement.set_clientip(client_ip);
      object_placement.set_clientport(client_port);
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          object_placement.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          object_placement.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          object_placement.add_blockkeys(t_stripe.blocks[i]->block_key);
          object_placement.add_blockids(t_stripe.blocks[i]->block_id);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->decodeAndGetObject(&decode_and_get, object_placement, &get_reply);
      if (status.ok())
      {
        std::cout << "[GET] getting value of " << key << std::endl;
      }
    }
    catch (std::exception &e)
    {
      std::cout << "getValue exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  int CoordinatorImpl::get_cluster_id_by_group_id(Stripe &t_stripe, int group_id)
  {
    int block_id = t_stripe.group_to_blocks[group_id][0];
    for (const auto *block : t_stripe.blocks)
    {
      if (block->block_id == block_id)
      {
        return block->map2cluster;
      }
    }
    return -1;
  }

  bool CoordinatorImpl::recovery_one_block_breakdown(int stripe_id, int failed_block_id, 
    std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
    std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time,
    double &dest_data_node_network_time, double &dest_data_node_disk_io_time)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    const bool azure_like_global_parity =
        is_azure_like_code(code_type) && failed_block_id >= m_sys_config->k &&
        failed_block_id < m_sys_config->k + m_sys_config->r;

    if (azure_like_global_parity || recovery_group_ids.size() == 1)
    {
      std::vector<int> all_data_block_ids;
      const std::vector<int> *helper_block_ids = nullptr;
      int group_id = -1;
      if (azure_like_global_parity)
      {
        all_data_block_ids.reserve(static_cast<size_t>(m_sys_config->k));
        for (int i = 0; i < m_sys_config->k; ++i)
          all_data_block_ids.push_back(i);
        helper_block_ids = &all_data_block_ids;
      }
      else
      {
        group_id = recovery_group_ids[0];
        helper_block_ids = &t_stripe.group_to_blocks[group_id];
      }
      const std::vector<int> &group_block_ids = *helper_block_ids;
      const int dest_cluster_id = t_stripe.blocks[failed_block_id]->map2cluster;
      auto cluster_to_blocks = azure_like_global_parity
                                   ? partition_data_blocks_by_cluster(t_stripe, m_sys_config->k)
                                   : partition_group_helpers_by_cluster(t_stripe, group_block_ids, failed_block_id);

      std::vector<int> remote_clusters;
      for (const auto &kv : cluster_to_blocks)
      {
        if (kv.first != dest_cluster_id)
          remote_clusters.push_back(kv.first);
      }

      if (remote_clusters.empty() || !is_azure_like_code(code_type))
      {
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;

        int chosen_cluster_id = dest_cluster_id;
        if (!is_azure_like_code(code_type))
          chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, group_id);
        std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(0);
        for (int bid : group_block_ids)
        {
          if (bid == failed_block_id)
            continue;
          Block *t_block = t_stripe.blocks[bid];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
        grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
        status = m_proxy_ptrs[chosen_proxy]->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          dest_data_node_network_time = recovery_reply.dest_data_node_network_time();
          dest_data_node_disk_io_time = recovery_reply.dest_data_node_disk_io_time();
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
          return true;
        }
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }

      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      const int cross_rack_num = static_cast<int>(remote_clusters.size());
      std::atomic<bool> recovery_ok{true};
      std::vector<std::thread> threads;
      cross_rack_network_time = 0;
      cross_rack_xor_time = 0;
      dest_data_node_network_time = 0;
      dest_data_node_disk_io_time = 0;

      std::cout << "[Coordinator] rack-partitioned "
                << (azure_like_global_parity ? "global-parity " : "")
                << "recovery (breakdown) of " << stripe_id << "_" << failed_block_id
                << " dest_cluster=" << dest_cluster_id << " cross_rack_num=" << cross_rack_num
                << std::endl;

      for (int remote_cid : remote_clusters)
      {
        threads.push_back(std::thread([this, &t_stripe, &cluster_to_blocks, remote_cid, failed_block_id,
                                       dest_proxy_ip, dest_proxy_port, &recovery_ok,
                                       &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time,
                                       &network_start_time, &network_end_time, &grpc_notify_time, &grpc_start_time,
                                       &data_node_grpc_notify_time, &data_node_grpc_start_time]() {
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          for (int bid : cluster_to_blocks.at(remote_cid))
          {
            Block *t_block = t_stripe.blocks[bid];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
          std::string helper_proxy = m_cluster_table[remote_cid].proxy_ip + ":" +
                                     std::to_string(m_cluster_table[remote_cid].proxy_port);
          grpc::Status st = m_proxy_ptrs[helper_proxy]->degradedReadBreakdown(
              &degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (st.ok())
          {
            std::lock_guard<std::mutex> lock(m_mutex);
            disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
            disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
            decode_start_time.push_back(degraded_read_reply.decode_start_time());
            decode_end_time.push_back(degraded_read_reply.decode_end_time());
            network_start_time.push_back(degraded_read_reply.network_start_time());
            network_end_time.push_back(degraded_read_reply.network_end_time());
            grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
            grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
            std::cout << "[Coordinator] partial from cluster " << remote_cid
                      << " for block " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            recovery_ok.store(false);
            std::cout << "[Coordinator] partial from cluster " << remote_cid
                      << " for block " << failed_block_id << " failed!" << std::endl;
          }
        }));
      }

      threads.push_back(std::thread([this, &t_stripe, &cluster_to_blocks, &remote_clusters, cross_rack_num,
                                     dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
                                     &recovery_ok, &disk_io_start_time, &disk_io_end_time, &decode_start_time,
                                     &decode_end_time, &network_start_time, &network_end_time,
                                     &cross_rack_network_time, &cross_rack_xor_time, &grpc_notify_time, &grpc_start_time,
                                     &data_node_grpc_notify_time, &data_node_grpc_start_time,
                                     &dest_data_node_network_time, &dest_data_node_disk_io_time]() {
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        for (int remote_cid : remote_clusters)
        {
          recovery_request.add_proxyip(m_cluster_table[remote_cid].proxy_ip);
          recovery_request.add_proxyport(m_cluster_table[remote_cid].proxy_port);
        }
        auto local_it = cluster_to_blocks.find(dest_cluster_id);
        if (local_it != cluster_to_blocks.end())
        {
          for (int bid : local_it->second)
          {
            Block *t_block = t_stripe.blocks[bid];
            recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
        }
        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
        std::string dest_proxy = dest_proxy_ip + ":" + std::to_string(dest_proxy_port);
        grpc::Status st = m_proxy_ptrs[dest_proxy]->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (st.ok())
        {
          std::lock_guard<std::mutex> lock(m_mutex);
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          dest_data_node_network_time = recovery_reply.dest_data_node_network_time();
          dest_data_node_disk_io_time = recovery_reply.dest_data_node_disk_io_time();
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          recovery_ok.store(false);
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }));

      for (auto &t : threads)
        t.join();
      return recovery_ok.load();
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::atomic<bool> recovery_ok{true};
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this,
          &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
          &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time, &recovery_ok
        ](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(is_azure_like_code(m_sys_config->CodeType) && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

          grpc::Status status = m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::lock_guard<std::mutex> lock(m_mutex);
            disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
            disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
            decode_start_time.push_back(degraded_read_reply.decode_start_time());
            decode_end_time.push_back(degraded_read_reply.decode_end_time());
            network_start_time.push_back(degraded_read_reply.network_start_time());
            network_end_time.push_back(degraded_read_reply.network_end_time());
            grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
            grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
    
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            recovery_ok.store(false);
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = count_recovery_helper_groups(recovery_group_ids, dest_group_id);
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
        &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
        &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time, &cross_rack_network_time, &cross_rack_xor_time,
        &dest_data_node_network_time, &dest_data_node_disk_io_time, &recovery_ok
        ](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if (skip_dest_local_helpers_for_azure_global(m_sys_config->CodeType, failed_block_id,
                                                       m_sys_config->k, m_sys_config->r))
            break;

          if(is_azure_like_code(m_sys_config->CodeType) && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        //std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();

        grpc::Status status = m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->recoveryBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::lock_guard<std::mutex> lock(m_mutex);
          grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count());
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          dest_data_node_network_time = recovery_reply.dest_data_node_network_time();
          dest_data_node_disk_io_time = recovery_reply.dest_data_node_disk_io_time();
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          recovery_ok.store(false);
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
      return recovery_ok.load();
    }
  }

  grpc::Status CoordinatorImpl::decodeTest(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    std::string code_type = m_sys_config->CodeType;
    int k = m_sys_config->k;
    int r = m_sys_config->r;
    int z = m_sys_config->z;
    int block_size = m_sys_config->BlockSize;
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    Stripe &t_stripe = m_stripe_table[stripe_id];

    std::vector<int> recovery_group_ids = get_recovery_group_ids(code_type, k, r, z, failed_block_id);
    std::vector<int> recovery_block_ids;
    for(int i = 0; i < recovery_group_ids.size(); i++){
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
      for(int j = 0; j < blockids.size(); j++){
        if(is_azure_like_code(m_sys_config->CodeType) && recovery_block_ids.size() == (k / z))
          break;
        if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
          continue;
        if(blockids[j] != failed_block_id){
          recovery_block_ids.push_back(blockids[j]);
        }
      }
    }
    int block_num = recovery_block_ids.size();
    unsigned char *recovery_data = static_cast<unsigned char*>(std::aligned_alloc(32, m_sys_config->BlockSize * block_num));
    std::vector<unsigned char *> recovery_data_ptrs;
    for(int i = 0; i < block_num; i++){
      recovery_data_ptrs.push_back(recovery_data + i * block_size);
    }
    
    unsigned char *res = static_cast<unsigned char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    if(is_azure_like_code(code_type)){
      decode_azure_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else if(code_type == "UniLRC"){
      decode_unilrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size);
    }
    else if(code_type == "OptimalLRC"){
      decode_optimal_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else if(code_type == "UniformLRC"){
      decode_uniform_lrc(k, r, z, block_num, &recovery_block_ids, recovery_data_ptrs.data(), res, block_size, failed_block_id);
    }
    else{
      std::cout << "[Coordinator] decodeTest: unknown code type!" << std::endl;
      return grpc::Status(grpc::INVALID_ARGUMENT, "unknown code type");
    }
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    std::cout << "[Coordinator] decodeTest took " << duration.count() << " seconds" << std::endl;
    degradedReadReply->set_decode_time(duration.count());
    delete[] res;
    delete[] recovery_data;

    return grpc::Status::OK;
  } 

  bool CoordinatorImpl::recovery_one_block(int stripe_id, int failed_block_id)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    // Azure-like / CordXueLRC global parity: repair from ALL data blocks partitioned by
    // physical rack. Each rack computes GF-weighted partial (decode_azure_lrc); dest XORs.
    const bool azure_like_global_parity =
        is_azure_like_code(code_type) && failed_block_id >= m_sys_config->k &&
        failed_block_id < m_sys_config->k + m_sys_config->r;

    if (azure_like_global_parity || recovery_group_ids.size() == 1)
    {
      std::vector<int> all_data_block_ids;
      const std::vector<int> *helper_block_ids = nullptr;
      int group_id = -1;
      if (azure_like_global_parity)
      {
        all_data_block_ids.reserve(static_cast<size_t>(m_sys_config->k));
        for (int i = 0; i < m_sys_config->k; ++i)
          all_data_block_ids.push_back(i);
        helper_block_ids = &all_data_block_ids;
      }
      else
      {
        group_id = recovery_group_ids[0];
        helper_block_ids = &t_stripe.group_to_blocks[group_id];
      }
      const std::vector<int> &group_block_ids = *helper_block_ids;
      // Dest rack = failed block's physical cluster (not merely group-mapped cluster).
      const int dest_cluster_id = t_stripe.blocks[failed_block_id]->map2cluster;
      auto cluster_to_blocks = azure_like_global_parity
                                   ? partition_data_blocks_by_cluster(t_stripe, m_sys_config->k)
                                   : partition_group_helpers_by_cluster(t_stripe, group_block_ids, failed_block_id);

      std::vector<int> remote_clusters;
      for (const auto &kv : cluster_to_blocks)
      {
        if (kv.first != dest_cluster_id)
          remote_clusters.push_back(kv.first);
      }

      // All helpers already on dest rack: keep single-proxy pull path.
      if (remote_clusters.empty() || !is_azure_like_code(code_type))
      {
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;

        int chosen_cluster_id = dest_cluster_id;
        if (!is_azure_like_code(code_type))
          chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, group_id);
        std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(0);
        for (int bid : group_block_ids)
        {
          if (bid == failed_block_id)
            continue;
          Block *t_block = t_stripe.blocks[bid];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }

        status = m_proxy_ptrs[chosen_proxy]->recovery(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
          return true;
        }
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }

      // Azure-like / CordXueLRC rack-partitioned repair:
      // - data/local: helpers of one logical group across racks (XOR partials)
      // - global parity: all data blocks across racks (GF-weighted partials; XOR merges)
      // Remote rack proxies: local DN read → decode_azure_lrc partial → send to dest.
      // Dest rack proxy: local helpers decode + XOR remote partials → write repaired block.
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      const int cross_rack_num = static_cast<int>(remote_clusters.size());
      std::atomic<bool> recovery_ok{true};
      std::vector<std::thread> threads;

      std::cout << "[Coordinator] rack-partitioned "
                << (azure_like_global_parity ? "global-parity " : "")
                << "recovery of " << stripe_id << "_" << failed_block_id
                << " dest_cluster=" << dest_cluster_id << " cross_rack_num=" << cross_rack_num
                << std::endl;

      for (int remote_cid : remote_clusters)
      {
        threads.push_back(std::thread([this, &t_stripe, &cluster_to_blocks, remote_cid, failed_block_id,
                                       dest_proxy_ip, dest_proxy_port, &recovery_ok]() {
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          for (int bid : cluster_to_blocks.at(remote_cid))
          {
            Block *t_block = t_stripe.blocks[bid];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::string helper_proxy = m_cluster_table[remote_cid].proxy_ip + ":" +
                                     std::to_string(m_cluster_table[remote_cid].proxy_port);
          grpc::Status st = m_proxy_ptrs[helper_proxy]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (st.ok())
          {
            std::cout << "[Coordinator] partial from cluster " << remote_cid
                      << " for block " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            recovery_ok.store(false);
            std::cout << "[Coordinator] partial from cluster " << remote_cid
                      << " for block " << failed_block_id << " failed!" << std::endl;
          }
        }));
      }

      threads.push_back(std::thread([this, &t_stripe, &cluster_to_blocks, &remote_clusters, cross_rack_num,
                                     dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
                                     &recovery_ok]() {
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        for (int remote_cid : remote_clusters)
        {
          recovery_request.add_proxyip(m_cluster_table[remote_cid].proxy_ip);
          recovery_request.add_proxyport(m_cluster_table[remote_cid].proxy_port);
        }
        auto local_it = cluster_to_blocks.find(dest_cluster_id);
        if (local_it != cluster_to_blocks.end())
        {
          for (int bid : local_it->second)
          {
            Block *t_block = t_stripe.blocks[bid];
            recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            recovery_request.add_blockkeys(t_block->block_key);
            recovery_request.add_blockids(t_block->block_id);
          }
        }
        std::string dest_proxy = dest_proxy_ip + ":" + std::to_string(dest_proxy_port);
        grpc::Status st = m_proxy_ptrs[dest_proxy]->recovery(&recovery_context, recovery_request, &recovery_reply);
        if (st.ok())
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          recovery_ok.store(false);
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }));

      for (auto &t : threads)
        t.join();
      return recovery_ok.load();
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::atomic<bool> recovery_ok{true};
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this, &recovery_ok](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(is_azure_like_code(m_sys_config->CodeType) && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          grpc::Status status = m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            recovery_ok.store(false);
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = count_recovery_helper_groups(recovery_group_ids, dest_group_id);
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, &recovery_group_ids, &recovery_ok](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::RecoveryReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        int t_node_id = randomly_select_a_node(dest_cluster_id, stripe_id);
        recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
        recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        for(int i = 0; i < recovery_group_ids.size(); i++){
          if(recovery_group_ids[i] == dest_group_id){
            continue;
          }
          int cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]);
          std::string proxy_ip = m_cluster_table[cluster_id].proxy_ip;
          int proxy_port = m_cluster_table[cluster_id].proxy_port;
          recovery_request.add_proxyip(proxy_ip);
          recovery_request.add_proxyport(proxy_port);
        }
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if (skip_dest_local_helpers_for_azure_global(m_sys_config->CodeType, failed_block_id,
                                                       m_sys_config->k, m_sys_config->r))
            break;

          if(is_azure_like_code(m_sys_config->CodeType) && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        //std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->recovery(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          recovery_ok.store(false);
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
      return recovery_ok.load();
    }
  }


  grpc::Status CoordinatorImpl::getRecoveryBreakdown(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RecoveryReply *recoveryReply)
  {
    std::chrono::time_point<std::chrono::high_resolution_clock> START = std::chrono::high_resolution_clock::now();
    recoveryReply->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::vector<double> disk_io_start_time, disk_io_end_time;
    std::vector<double> decode_start_time, decode_end_time;
    std::vector<double> network_start_time, network_end_time;
    double cross_rack_network_time, cross_rack_xor_time;
    std::vector<double> grpc_notify_time, grpc_start_time;
    std::vector<double> data_node_grpc_notify_time, data_node_grpc_start_time;
    double dest_data_node_network_time, dest_data_node_disk_io_time;

    bool if_success = recovery_one_block_breakdown(stripe_id, failed_block_id, 
      disk_io_start_time, disk_io_end_time, decode_start_time, decode_end_time,
      network_start_time, network_end_time, cross_rack_network_time, cross_rack_xor_time,
      grpc_notify_time, grpc_start_time, data_node_grpc_notify_time, data_node_grpc_start_time,
      dest_data_node_network_time, dest_data_node_disk_io_time);

    if (if_success)
    {
      double max_disk_io_time = *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) - *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
      recoveryReply->set_disk_read_time(max_disk_io_time);
      double max_decode_time = *std::max_element(decode_end_time.begin(), decode_end_time.end()) - *std::min_element(decode_start_time.begin(), decode_start_time.end());
      recoveryReply->set_decode_time(max_decode_time + cross_rack_xor_time);
      double max_network_time = *std::max_element(network_end_time.begin(), network_end_time.end()) - *std::min_element(network_start_time.begin(), network_start_time.end());
      double max_grpc_delay = *std::max_element(grpc_start_time.begin(), grpc_start_time.end()) - *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end());
      double max_data_node_grpc_delay = *std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()) - *std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end());
      recoveryReply->set_network_time(max_network_time + cross_rack_network_time + dest_data_node_network_time + max_grpc_delay + max_data_node_grpc_delay);
      recoveryReply->set_disk_write_time(dest_data_node_disk_io_time);
      
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
    }
  }

  grpc::Status CoordinatorImpl::getRecovery(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RecoveryReply *recoveryReply)
  {
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    bool if_success = recovery_one_block(stripe_id, failed_block_id);

    if (if_success)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Recovery failed!");
    }
  }

  bool CoordinatorImpl::degraded_read_one_block_breakdown(int stripe_id, int failed_block_id, std::string client_ip, int client_port, 
    std::vector<double> &disk_io_start_time, std::vector<double> &disk_io_end_time, std::vector<double> &decode_start_time, std::vector<double> &decode_end_time,
    std::vector<double> &network_start_time, std::vector<double> &network_end_time, double &cross_rack_network_time, double &cross_rack_xor_time,
    std::vector<double> &grpc_notify_time, std::vector<double> &grpc_start_time, std::vector<double> &data_node_grpc_notify_time, std::vector<double> &data_node_grpc_start_time)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }

      std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
      grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2ClientBreakdown(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
        disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
        decode_start_time.push_back(degraded_read_reply.decode_start_time());
        decode_end_time.push_back(degraded_read_reply.decode_end_time());
        network_start_time.push_back(degraded_read_reply.network_start_time());
        network_end_time.push_back(degraded_read_reply.network_end_time());
        cross_rack_network_time = degraded_read_reply.cross_rack_time();
        cross_rack_xor_time = degraded_read_reply.cross_rack_xor_time();
        grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
        data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
        data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this, 
          &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
          &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(is_azure_like_code(m_sys_config->CodeType) && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
          grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedReadBreakdown(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            disk_io_start_time.push_back(degraded_read_reply.disk_io_start_time());
            disk_io_end_time.push_back(degraded_read_reply.disk_io_end_time());
            decode_start_time.push_back(degraded_read_reply.decode_start_time());
            decode_end_time.push_back(degraded_read_reply.decode_end_time());
            network_start_time.push_back(degraded_read_reply.network_start_time());
            network_end_time.push_back(degraded_read_reply.network_end_time());
            grpc_start_time.push_back(degraded_read_reply.grpc_start_time());
            data_node_grpc_notify_time.push_back(degraded_read_reply.data_node_grpc_notify_time());
            data_node_grpc_start_time.push_back(degraded_read_reply.data_node_grpc_start_time());
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = count_recovery_helper_groups(recovery_group_ids, dest_group_id);
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, 
        &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, &cross_rack_network_time, &cross_rack_xor_time,
        &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if (skip_dest_local_helpers_for_azure_global(m_sys_config->CodeType, failed_block_id,
                                                       m_sys_config->k, m_sys_config->r))
            break;

          if(is_azure_like_code(m_sys_config->CodeType) && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
        grpc_notify_time.push_back(std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count());
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2ClientBreakdown(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          disk_io_start_time.push_back(recovery_reply.disk_io_start_time());
          disk_io_end_time.push_back(recovery_reply.disk_io_end_time());
          decode_start_time.push_back(recovery_reply.decode_start_time());
          decode_end_time.push_back(recovery_reply.decode_end_time());
          network_start_time.push_back(recovery_reply.network_start_time());
          network_end_time.push_back(recovery_reply.network_end_time());
          cross_rack_network_time = recovery_reply.cross_rack_time();
          cross_rack_xor_time = recovery_reply.cross_rack_xor_time();
          grpc_start_time.push_back(recovery_reply.grpc_start_time());
          data_node_grpc_notify_time.push_back(recovery_reply.data_node_grpc_notify_time());
          data_node_grpc_start_time.push_back(recovery_reply.data_node_grpc_start_time());
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }

  bool CoordinatorImpl::degraded_read_one_block(int stripe_id, int failed_block_id, std::string client_ip, int client_port)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(is_azure_like_code(m_sys_config->CodeType) && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = count_recovery_helper_groups(recovery_group_ids, dest_group_id);
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if (skip_dest_local_helpers_for_azure_global(m_sys_config->CodeType, failed_block_id,
                                                       m_sys_config->k, m_sys_config->r))
            break;

          if(is_azure_like_code(m_sys_config->CodeType) && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2Client(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }

  bool CoordinatorImpl::degraded_read_one_block_for_workload(int stripe_id, int failed_block_id, std::string client_ip, int client_port, int block_id)
  {
    std::string code_type = m_sys_config->CodeType;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    std::vector<int> recovery_group_ids = get_recovery_group_ids(m_sys_config->CodeType, m_sys_config->k, m_sys_config->r, m_sys_config->z, failed_block_id);
    grpc::Status status;

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::DegradedReadReply degraded_read_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      recovery_request.set_replaced_node_ip(client_ip);
      recovery_request.set_replaced_node_port(client_port);
      recovery_request.set_cross_rack_num(0);
      recovery_request.set_is_to_send_block_id(true);
      recovery_request.set_block_id_to_send(block_id);
      std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[0]];
      for (int i = 0; i < int(blockids.size()); i++)
      {
        if (blockids[i] == failed_block_id)
          continue;

        Block *t_block = t_stripe.blocks[blockids[i]];
        recovery_request.add_datanodeip(m_node_table[t_block->map2node].node_ip);
        recovery_request.add_datanodeport(m_node_table[t_block->map2node].node_port);
        recovery_request.add_blockkeys(t_block->block_key);
        recovery_request.add_blockids(t_block->block_id);
      }
      status = m_proxy_ptrs[chosen_proxy]->degradedRead2Client(&recovery_context, recovery_request, &degraded_read_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        return false;
      }
    }
    else
    {
      int dest_group_id = t_stripe.blocks[failed_block_id]->map2group;
      int dest_cluster_id = get_cluster_id_by_group_id(t_stripe, dest_group_id);
      std::string dest_proxy_ip = m_cluster_table[dest_cluster_id].proxy_ip;
      int dest_proxy_port = m_cluster_table[dest_cluster_id].proxy_port;
      std::vector<int> chosen_cluster_ids;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        chosen_cluster_ids.push_back(get_cluster_id_by_group_id(t_stripe, recovery_group_ids[i]));
      }
      std::vector<std::string> chosen_proxies;
      for(int i = 0; i < chosen_cluster_ids.size(); i++){
        chosen_proxies.push_back(m_cluster_table[chosen_cluster_ids[i]].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_ids[i]].proxy_port));
      }
      std::vector<std::thread> threads;
      for(int i = 0; i < recovery_group_ids.size(); i++){
        if(recovery_group_ids[i] == dest_group_id){
          continue;
        }
        threads.push_back(std::thread([&t_stripe, &chosen_proxies, &recovery_group_ids, i, failed_block_id, dest_proxy_ip, dest_proxy_port, this](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::RECOVERY_XFER_PORT_OFFSET);
          degraded_read_request.set_failed_block_id(failed_block_id);
          degraded_read_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
          std::vector<int> blockids = t_stripe.group_to_blocks[recovery_group_ids[i]];
          for (int j = 0; j < int(blockids.size()); j++)
          {
            if(is_azure_like_code(m_sys_config->CodeType) && degraded_read_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
              break;

            if ((is_azure_like_code(m_sys_config->CodeType) && blockids[j] >= m_sys_config->k + m_sys_config->r) || blockids[j] == failed_block_id)
              continue;

            Block *t_block = t_stripe.blocks[blockids[j]];
            degraded_read_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
            degraded_read_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
            degraded_read_request.add_blockkeys(t_block->block_key);
            degraded_read_request.add_blockids(t_block->block_id);
          }
          std::cout << "[Coordinator] start partial degraded read of " << failed_block_id << std::endl;
          grpc::Status status = this->m_proxy_ptrs[chosen_proxies[i]]->degradedRead(&degraded_read_context, degraded_read_request, &degraded_read_reply);
          if (status.ok())
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " success!" << std::endl;
          }
          else
          {
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = count_recovery_helper_groups(recovery_group_ids, dest_group_id);
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, block_id](){
        grpc::ClientContext recovery_context;
        proxy_proto::RecoveryRequest recovery_request;
        proxy_proto::DegradedReadReply recovery_reply;
        recovery_request.set_failed_block_id(failed_block_id);
        recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
        recovery_request.set_replaced_node_ip(client_ip);
        recovery_request.set_replaced_node_port(client_port);
        recovery_request.set_cross_rack_num(cross_rack_num);
        recovery_request.set_is_to_send_block_id(true);
        recovery_request.set_block_id_to_send(block_id);
        std::vector<int> blockids = t_stripe.group_to_blocks[dest_group_id];
        for (int i = 0; i < int(blockids.size()); i++)
        {
          if (skip_dest_local_helpers_for_azure_global(m_sys_config->CodeType, failed_block_id,
                                                       m_sys_config->k, m_sys_config->r))
            break;

          if(is_azure_like_code(m_sys_config->CodeType) && recovery_request.blockids_size() == (m_sys_config->k / m_sys_config->z))
            break;

          if (blockids[i] == failed_block_id)
            continue;

          Block *t_block = t_stripe.blocks[blockids[i]];
          recovery_request.add_datanodeip(this->m_node_table[t_block->map2node].node_ip);
          recovery_request.add_datanodeport(this->m_node_table[t_block->map2node].node_port);
          recovery_request.add_blockkeys(t_block->block_key);
          recovery_request.add_blockids(t_block->block_id);
        }
        std::cout << "[Coordinator] start recovery of " << stripe_id << "_" << failed_block_id << std::endl;
        grpc::Status status = this->m_proxy_ptrs[dest_proxy_ip + ":" + std::to_string(dest_proxy_port)]->degradedRead2Client(&recovery_context, recovery_request, &recovery_reply);
        if (status.ok())
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        }
        else
        {
          std::cout << "[Coordinator] degraded read of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
  }


  grpc::Status CoordinatorImpl::getDegradedReadBlockBreakdown(
    grpc::ServerContext *context,
    const coordinator_proto::KeyAndClientIP *keyClient,
    coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    double start_time = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
    degradedReadReply->set_grpc_start_time(start_time);
    std::cout << start_time << std::endl;
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::string client_ip = keyClient->clientip();
    int client_port = keyClient->clientport();
    std::vector<double> disk_io_start_time;
    std::vector<double> disk_io_end_time;
    std::vector<double> decode_start_time;
    std::vector<double> decode_end_time;
    std::vector<double> network_start_time;
    std::vector<double> network_end_time;
    std::vector<double> grpc_notify_time;
    std::vector<double> grpc_start_time;
    std::vector<double> data_node_grpc_notify_time;
    std::vector<double> data_node_grpc_start_time;
    double cross_rack_network_time;
    double cross_rack_xor_time;

    /*std::thread t(&CoordinatorImpl::degraded_read_one_block, this, stripe_id, failed_block_id, client_ip, client_port);
    t.join();*/
    bool if_success = degraded_read_one_block_breakdown(stripe_id, failed_block_id, client_ip, client_port,
      disk_io_start_time, disk_io_end_time, decode_start_time, decode_end_time,
      network_start_time, network_end_time, cross_rack_network_time, cross_rack_xor_time,
      grpc_notify_time, grpc_start_time, data_node_grpc_notify_time, data_node_grpc_start_time); 
    if (if_success)
    {
      double max_disk_io_time = *std::max_element(disk_io_end_time.begin(), disk_io_end_time.end()) - *std::min_element(disk_io_start_time.begin(), disk_io_start_time.end());
      double max_decode_time = *std::max_element(decode_end_time.begin(), decode_end_time.end()) - *std::min_element(decode_start_time.begin(), decode_start_time.end()) + cross_rack_xor_time;
      double max_network_time = *std::max_element(network_end_time.begin(), network_end_time.end()) - *std::min_element(network_start_time.begin(), network_start_time.end()) + cross_rack_network_time;
      max_network_time += (*std::max_element(grpc_start_time.begin(), grpc_start_time.end()) - *std::min_element(grpc_notify_time.begin(), grpc_notify_time.end()));
      max_network_time += (*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()) - *std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));

      degradedReadReply->set_disk_io_time(max_disk_io_time);
      degradedReadReply->set_decode_time(max_decode_time);
      degradedReadReply->set_network_time(max_network_time);
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
    }
  }


  grpc::Status CoordinatorImpl::getDegradedReadBlock(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::DegradedReadReply *degradedReadReply)
  {
    int stripe_id = std::stoi(keyClient->key().substr(0, keyClient->key().find('_')));
    int failed_block_id = std::stoi(keyClient->key().substr(keyClient->key().find('_') + 1));
    std::string client_ip = keyClient->clientip();
    int client_port = keyClient->clientport();

    bool if_success = degraded_read_one_block(stripe_id, failed_block_id, client_ip, client_port);
    if (if_success)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Degraded read failed!");
    }
  }

  grpc::Status CoordinatorImpl::fullNodeRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::NodeIdFromClient *request,
    coordinator_proto::RepBlockNum* response)
  {
    int node_id = request->node_id();
    std::string node_ip = m_node_table[node_id].node_ip;
    std::vector<int> stripe_ids;
    std::vector<int> block_ids;
    for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
    {
      for (int i = 0; i < int(it->second.blocks.size()); i++)
      {
        if (it->second.blocks[i]->map2node == node_id)
        {
          stripe_ids.push_back(it->first);
          block_ids.push_back(it->second.blocks[i]->block_id);
        }
      }
    }
    if(stripe_ids.size() == 0){
      std::cout << "[Coordinator] no blocks on node " << node_id << std::endl;
      return grpc::Status::OK;
    }
    std::cout << "[Coordinator] start full node recovery of " << node_id << " containing " << block_ids.size() << " blocks" << std::endl;
    response->set_block_num(block_ids.size());
    //recovery_full_node(stripe_ids, block_ids);
    //std::vector<std::thread> recovery_threads;
    std::vector<bool> recovery_results(stripe_ids.size(), false);
    for (int i = 0; i < stripe_ids.size(); i++) {
        bool result = this->recovery_one_block(stripe_ids[i], block_ids[i]);
        recovery_results[i] = result; // 保存结果
    }
  
        
    // 检查结果
    bool all_success = std::all_of(recovery_results.begin(), recovery_results.end(), [](bool res) { return res; });
    if (all_success) {
        std::cout << "All recovery operations succeeded!" << std::endl;
    } else {
        std::cout << "Some recovery operations failed!" << std::endl;
    }


    /*bool ifSuccess = recovery_full_node(stripe_ids, block_ids);
    if (ifSuccess)
    {
      return grpc::Status::OK;
    }
    else
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Full node recovery failed!");
    }*/
    return grpc::Status::OK;
  } 

  grpc::Status CoordinatorImpl::multiBlockRecovery(
    grpc::ServerContext *context,
    const coordinator_proto::StripeIdAndBlockIDsFromClient *request,
    coordinator_proto::RecoveryReply *replyClient)
  {
    int stripe_id = request->stripe_id();
    int block_num = request->block_ids_size();
    std::vector<int> block_ids;
    for(int i = 0; i < block_num; i++){
      block_ids.push_back(request->block_ids(i));
    }
    std::vector<int> node_ids;
    for(int i = 0; i < block_ids.size(); i++){
      node_ids.push_back(m_stripe_table[stripe_id].blocks[block_ids[i]]->map2node);
    }
    int chosen_cluster_id = randomly_select_a_cluster(stripe_id);
    int chosen_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
    std::vector<int> decode_block_ids;
    std::vector<std::vector<int>> decode_factors;
    bool ifGetDecodePlanSuccess = ECProject::get_multi_decode_plan(m_sys_config->k, m_sys_config->r, m_sys_config->z, m_sys_config->CodeType, block_ids, decode_block_ids, decode_factors);
    if(!ifGetDecodePlanSuccess){
      std::cout << "[Coordinator] get multi decode plan failed!" << std::endl;
      return grpc::Status(grpc::StatusCode::INTERNAL, "Get multi decode plan failed!");
    }
    std::cout << "[Coordinator] get multi decode plan success!" << std::endl;
    // TODO: notify source proxies and dest proxy, including partial decoding...
    std::vector<int> source_proxies;
    std::vector<std::vector<int>> source_datanodes;
    std::vector<std::vector<int>> decode_blocks_split_for_proxies;
    std::vector<std::vector<int>> decode_factors_split_for_proxies;
    for(int i = 0; i < decode_block_ids.size(); i++){
      int source_proxy = m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2cluster;
      auto it = std::find(source_proxies.begin(), source_proxies.end(), source_proxy);
      if(it == source_proxies.end()){
        source_proxies.push_back(source_proxy);
        std::vector<int> t_source_datanodes;
        t_source_datanodes.push_back(m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2node);
        source_datanodes.push_back(t_source_datanodes);
        std::vector<int> t_decode_blocks;
        t_decode_blocks.push_back(decode_block_ids[i]);
        decode_blocks_split_for_proxies.push_back(t_decode_blocks);
        std::vector<int> t_decode_factors;
        t_decode_factors.push_back(decode_factors[i][0]);
        decode_factors_split_for_proxies.push_back(t_decode_factors);
      }
      else{
        int index = std::distance(source_proxies.begin(), it);
        source_datanodes[index].push_back(m_stripe_table[stripe_id].blocks[decode_block_ids[i]]->map2node);
        decode_blocks_split_for_proxies[index].push_back(decode_block_ids[i]);
        decode_factors_split_for_proxies[index].push_back(decode_factors[i][0]);
      }
    }
    (void)node_ids;
    (void)chosen_node_id;
    (void)source_proxies;
    (void)source_datanodes;
    (void)decode_blocks_split_for_proxies;
    (void)decode_factors_split_for_proxies;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByKey(
      grpc::ServerContext *context,
      const coordinator_proto::KeyFromClient *del_key,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      std::string key = del_key->key();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_object_updating_table[key] = m_object_commit_table[key];
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      node_block.set_stripe_id(-1); // as a flag to distinguish delete key or stripe
      node_block.set_key(key);
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of " << key << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByKey exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByStripe(
      grpc::ServerContext *context,
      const coordinator_proto::StripeIdFromClient *stripeid,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      int t_stripe_id = stripeid->stripe_id();
      m_mutex.lock();
      m_stripe_deleting_table.push_back(t_stripe_id);
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[t_stripe_id];
      std::unordered_set<int> t_cluster_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2stripe == t_stripe_id)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_cluster_set.insert(t_stripe.blocks[i]->map2cluster);
        }
      }
      node_block.set_stripe_id(t_stripe_id);
      node_block.set_key("");
      // randomly select a cluster
      int idx = rand_num(int(t_cluster_set.size()));
      int r_cluster_id = *(std::next(t_cluster_set.begin(), idx));
      std::string chosen_proxy = m_cluster_table[r_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[r_cluster_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of Stripe " << t_stripe_id << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByStripe exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::listStripes(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *req,
      coordinator_proto::RepStripeIds *listReplyClient)
  {
    try
    {
      for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
      {
        listReplyClient->add_stripe_ids(it->first);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::checkalive(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {

    std::cout << "[Coordinator Check] alive " << helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }
  grpc::Status CoordinatorImpl::reportCommitAbort(
      grpc::ServerContext *context,
      const coordinator_proto::CommitAbortKey *commit_abortkey,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string key = commit_abortkey->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)commit_abortkey->opp();
    int stripe_id = commit_abortkey->stripe_id();
    std::unique_lock<std::mutex> lck(m_mutex);
    try
    {
      if (commit_abortkey->ifcommitmetadata())
      {
        if (opp == SET || opp == APPEND || opp == CORD_UPDATE)
        {
          m_object_commit_table[key] = m_object_updating_table[key];
          cv.notify_all();
          m_object_updating_table.erase(key);
          if (opp == CORD_UPDATE)
          {
            // Release m_mutex before auto-starting transfer plan so checkCommitAbort
            // can return immediately after commit without waiting for cross-cluster gRPC.
            lck.unlock();
            cord_on_delta_key_committed(key);
          }
        }
        else if (opp == DEL) // delete the metadata
        {
          if (stripe_id < 0) // delete key
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete key finish!" << std::endl;
            }
            ObjectInfo object_info = m_object_commit_table.at(key);
            stripe_id = object_info.map2stripe;
            m_object_commit_table.erase(key); // update commit table
            cv.notify_all();
            m_object_updating_table.erase(key);
            Stripe &t_stripe = m_stripe_table[stripe_id];
            std::vector<Block *>::iterator it1;
            for (it1 = t_stripe.blocks.begin(); it1 != t_stripe.blocks.end();)
            {
              if ((*it1)->map2key == key)
              {
                it1 = t_stripe.blocks.erase(it1);
              }
              else
              {
                it1++;
              }
            }
            if (t_stripe.blocks.empty()) // update stripe table
            {
              m_stripe_table.erase(stripe_id);
            }
            std::map<int, Cluster>::iterator it2; // update cluster table
            for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end(); it2++)
            {
              Cluster &t_cluster = it2->second;
              for (it1 = t_cluster.blocks.begin(); it1 != t_cluster.blocks.end();)
              {
                if ((*it1)->map2key == key)
                {
                  update_stripe_info_in_node(false, (*it1)->map2node, (*it1)->map2stripe); // update node table
                  it1 = t_cluster.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
          } // delete stripe
          else
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete stripe finish!" << std::endl;
            }
            auto its = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
            if (its != m_stripe_deleting_table.end())
            {
              m_stripe_deleting_table.erase(its);
            }
            cv.notify_all();
            // update stripe table
            m_stripe_table.erase(stripe_id);
            std::unordered_set<std::string> object_keys_set;
            // update cluster table
            std::map<int, Cluster>::iterator it2;
            for (it2 = m_cluster_table.begin(); it2 != m_cluster_table.end(); it2++)
            {
              Cluster &t_cluster = it2->second;
              for (auto it1 = t_cluster.blocks.begin(); it1 != t_cluster.blocks.end();)
              {
                if ((*it1)->map2stripe == stripe_id)
                {
                  object_keys_set.insert((*it1)->map2key);
                  it1 = t_cluster.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
            // update node table
            for (auto it3 = m_node_table.begin(); it3 != m_node_table.end(); it3++)
            {
              Node &t_node = it3->second;
              auto it4 = t_node.stripes.find(stripe_id);
              if (it4 != t_node.stripes.end())
              {
                t_node.stripes.erase(stripe_id);
              }
            }
            // update commit table
            for (auto it5 = object_keys_set.begin(); it5 != object_keys_set.end(); it5++)
            {
              auto it6 = m_object_commit_table.find(*it5);
              if (it6 != m_object_commit_table.end())
              {
                m_object_commit_table.erase(it6);
              }
            }
            // merge group
          }
          // if (IF_DEBUG)
          // {
          //   std::cout << "[DEL] Data placement after delete:" << std::endl;
          //   for (int i = 0; i < m_num_of_Clusters; i++)
          //   {
          //     Cluster &t_cluster = m_cluster_table[i];
          //     if (int(t_cluster.blocks.size()) > 0)
          //     {
          //       std::cout << "Cluster " << i << ": ";
          //       for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end(); it++)
          //       {
          //         std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
          //       }
          //       std::cout << std::endl;
          //     }
          //   }
          //   std::cout << std::endl;
          // }
        }
      }
      else
      {
        m_object_updating_table.erase(key);
      }
    }
    catch (std::exception &e)
    {
      std::cout << "reportCommitAbort exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status
  CoordinatorImpl::checkCommitAbort(grpc::ServerContext *context,
                                    const coordinator_proto::AskIfSuccess *key_opp,
                                    coordinator_proto::RepIfSuccess *reply)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    std::string key = key_opp->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)key_opp->opp();
    int stripe_id = key_opp->stripe_id();
    if (opp == SET || opp == APPEND || opp == CORD_UPDATE)
    {
      while (m_object_commit_table.find(key) == m_object_commit_table.end())
      {
        cv.wait(lck);
      }
    }
    else if (opp == DEL)
    {
      if (stripe_id < 0)
      {
        while (m_object_commit_table.find(key) != m_object_commit_table.end())
        {
          cv.wait(lck);
        }
      }
      else
      {
        auto it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        while (it != m_stripe_deleting_table.end())
        {
          cv.wait(lck);
          it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        }
      }
    }
    reply->set_ifcommit(true);
    return grpc::Status::OK;
  }

  // Check the connnection to all proxies of all clusters
  bool CoordinatorImpl::init_proxyinfo()
  {
    for (auto cur = m_cluster_table.begin(); cur != m_cluster_table.end(); cur++)
    {
      std::string proxy_ip_and_port = cur->second.proxy_ip + ":" + std::to_string(cur->second.proxy_port);
      auto _stub = proxy_proto::proxyService::NewStub(grpc::CreateChannel(proxy_ip_and_port, grpc::InsecureChannelCredentials()));
      proxy_proto::CheckaliveCMD Cmd;
      proxy_proto::RequestResult result;
      grpc::ClientContext clientContext;
      Cmd.set_name("coordinator");
      grpc::Status status;
      status = _stub->checkalive(&clientContext, Cmd, &result);
      if (status.ok())
      {
        std::cout << "[Proxy Check] ok from " << proxy_ip_and_port << std::endl;
      }
      else
      {
        std::cout << "[Proxy Check] failed to connect " << proxy_ip_and_port << std::endl;
      }
      m_proxy_ptrs.insert(std::make_pair(proxy_ip_and_port, std::move(_stub)));
    }
    return true;
  }
  bool CoordinatorImpl::init_clusterinfo(std::string m_clusterinfo_path)
  {
    std::cout << "Cluster_information_path:" << m_clusterinfo_path << std::endl;
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_clusterinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    int node_id = 0;
    m_num_of_Clusters = 0;
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      std::cout << "cluster_id: " << cluster_id << " , proxy: " << proxy << std::endl;
      Cluster t_cluster;
      m_cluster_table[std::stoi(cluster_id)] = t_cluster;
      m_cluster_table[std::stoi(cluster_id)].cluster_id = std::stoi(cluster_id);
      auto pos = proxy.find(':');
      m_cluster_table[std::stoi(cluster_id)].proxy_ip = proxy.substr(0, pos);
      m_cluster_table[std::stoi(cluster_id)].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        std::cout << "____node: " << node_uri << std::endl;
        m_cluster_table[std::stoi(cluster_id)].nodes.push_back(node_id);
        m_node_table[node_id].node_id = node_id;
        auto pos = node_uri.find(':');
        m_node_table[node_id].node_ip = node_uri.substr(0, pos);
        m_node_table[node_id].node_port = std::stoi(node_uri.substr(pos + 1, node_uri.size()));
        m_node_table[node_id].cluster_id = std::stoi(cluster_id);
        node_id++;
      }
      m_num_of_Clusters++;
    }
    return true;
  }

  int CoordinatorImpl::randomly_select_a_cluster(int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    return randomly_select_a_cluster(stripe_id, gen);
  }

  int CoordinatorImpl::randomly_select_a_cluster(int stripe_id, std::mt19937 &gen)
  {
    std::uniform_int_distribution<int> dis_cluster(0, m_num_of_Clusters - 1);
    int r_cluster_id = dis_cluster(gen);
    while (m_cluster_table[r_cluster_id].stripes.find(stripe_id) != m_cluster_table[r_cluster_id].stripes.end())
    {
      r_cluster_id = dis_cluster(gen);
    }
    return r_cluster_id;
  }

  // randomly select a node in the selected cluster
  // with the constraint that the node has not been selected for the same stripe
  int CoordinatorImpl::randomly_select_a_node(int cluster_id, int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    return randomly_select_a_node(cluster_id, stripe_id, gen);
  }

  int CoordinatorImpl::randomly_select_a_node(int cluster_id, int stripe_id, std::mt19937 &gen)
  {
    std::uniform_int_distribution<int> dis_node(0, m_cluster_table[cluster_id].nodes.size() - 1);
    int r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
    while (m_node_table[r_node_id].stripes.find(stripe_id) != m_node_table[r_node_id].stripes.end())
    {
      r_node_id = m_cluster_table[cluster_id].nodes[dis_node(gen)];
    }
    return r_node_id;
  }

  void CoordinatorImpl::update_stripe_info_in_node(int t_node_id, int stripe_id, int index)
  {
    assert(m_node_table[t_node_id].stripes.find(stripe_id) == m_node_table[t_node_id].stripes.end() && "The node has been selected for the stripe");
    m_node_table[t_node_id].stripes[stripe_id] = index;
  }

  // maintain the block number of the stripe in the node
  // TODO: Still don't konw why the stripe_block_num is start from 1
  void
  CoordinatorImpl::update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id)
  {
    int stripe_block_num = 1;
    if (m_node_table[t_node_id].stripes.find(stripe_id) != m_node_table[t_node_id].stripes.end())
    {
      stripe_block_num = m_node_table[t_node_id].stripes[stripe_id];
    }
    if (add_or_sub)
    {
      m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num + 1;
    }
    else
    {
      if (stripe_block_num == 1)
      {
        m_node_table[t_node_id].stripes.erase(stripe_id);
      }
      else
      {
        m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num - 1;
      }
    }
  }

  int CoordinatorImpl::generate_placement(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k = stripe_info.k;
    int l = stripe_info.l;
    int g_m = stripe_info.g_m;
    int b = m_encode_parameters.b_datapergroup;
    ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[k + g_m + l];
    for (int i = 0; i < k + g_m + l; i++)
    {
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / b);
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= k && i < k + g_m)
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i - k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = l;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_L" + std::to_string(i - k - g_m);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - k - g_m;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
    }

    if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
    {
      if (s_placement_type == Optimal)
      {
        if (m_placement_type == Ran)
        {
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_cluster_id = -1;
          std::mt19937 encode_ran_gen_storage;
          std::mt19937 *encode_ran_gen = nullptr;
          if (m_sys_config->PlacementRandomSeed != 0ULL)
          {
            seed_placement_mt19937(encode_ran_gen_storage, m_sys_config->PlacementRandomSeed, stripe_id);
            encode_ran_gen = &encode_ran_gen_storage;
          }
          auto pick_cluster = [&](int sid) -> int {
            return encode_ran_gen ? randomly_select_a_cluster(sid, *encode_ran_gen) : randomly_select_a_cluster(sid);
          };
          auto pick_node = [&](int cid, int sid) -> int {
            return encode_ran_gen ? randomly_select_a_node(cid, sid, *encode_ran_gen) : randomly_select_a_node(cid, sid);
          };
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a cluster
              int t_cluster_id = pick_cluster(stripe_id);
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = pick_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = pick_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1) // randomly select a new cluster
                  {
                    g_cluster_id = pick_cluster(stripe_id);
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = pick_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1) // randomly select a new cluster
          {
            g_cluster_id = pick_cluster(stripe_id);
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = pick_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == DIS)
        {
          int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
          int idx = m_merge_groups.size() - 1;
          if (b % (g_m + 1) == 0)
            required_cluster_num -= l;
          if (int(m_free_clusters.size()) < required_cluster_num || m_free_clusters.empty() || idx < 0 ||
              int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_free_clusters.clear();
            m_free_clusters.shrink_to_fit();
            for (int i = 0; i < m_num_of_Clusters; i++)
            {
              m_free_clusters.push_back(i);
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_cluster_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a cluster
              int t_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
              auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), t_cluster_id);
              if (iter != m_free_clusters.end())
              {
                m_free_clusters.erase(iter);
              } // remove the selected cluster from the free list
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1) // randomly select a new cluster
                  {
                    g_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
                    auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), g_cluster_id);
                    if (iter != m_free_clusters.end())
                    {
                      m_free_clusters.erase(iter);
                    }
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1) // randomly select a new cluster
          {
            g_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
            auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), g_cluster_id);
            if (iter != m_free_clusters.end())
            {
              m_free_clusters.erase(iter);
            }
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == AGG)
        {
          int agg_clusters_num = ceil(b + 1, g_m + 1) * l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_clusters_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
            m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }
          int t_cluster_id = m_agg_start_cid - 1;
          int g_cluster_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              t_cluster_id += 1;
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_cluster_id == -1)
                  {
                    g_cluster_id = t_cluster_id + 1;
                    t_cluster_id++;
                  }
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          if (g_cluster_id == -1)
          {
            g_cluster_id = t_cluster_id + 1;
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
        else if (m_placement_type == OPT)
        {
          int required_cluster_num = ceil(b + 1, g_m + 1) * l + 1;
          int agg_clusters_num = l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_clusters_num = 1;
            required_cluster_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (int(m_free_clusters.size()) < required_cluster_num - agg_clusters_num || m_free_clusters.empty() ||
              idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_agg_start_cid = rand_num(m_num_of_Clusters - agg_clusters_num);
            m_free_clusters.clear();
            m_free_clusters.shrink_to_fit();
            for (int i = 0; i < m_agg_start_cid; i++)
            {
              m_free_clusters.push_back(i);
            }
            for (int i = m_agg_start_cid + agg_clusters_num; i < m_num_of_Clusters; i++)
            {
              m_free_clusters.push_back(i);
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int agg_cluster_id = m_agg_start_cid - 1;
          int t_cluster_id = -1;
          int g_cluster_id = m_agg_start_cid + agg_clusters_num - 1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              if (flag && j + g_m + 1 != (i + 1) * b)
              {
                t_cluster_id = ++agg_cluster_id;
              }
              else
              {
                t_cluster_id = m_free_clusters[rand_num(int(m_free_clusters.size()))];
                auto iter = std::find(m_free_clusters.begin(), m_free_clusters.end(), t_cluster_id);
                if (iter != m_free_clusters.end())
                {
                  m_free_clusters.erase(iter);
                }
              }
              Cluster &t_cluster = m_cluster_table[t_cluster_id];
              // place every g+1 data blocks from each group to a single cluster
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected cluster
                int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                blocks_info[o].map2cluster = t_cluster_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_cluster.blocks.push_back(&blocks_info[o]);
                t_cluster.stripes.insert(stripe_id);
                stripe_info.place2clusters.insert(t_cluster_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected cluster
                  int t_node_id = randomly_select_a_node(t_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = t_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(t_cluster_id);
                }
                else // place the local parity blocks together with global ones
                {
                  Cluster &g_cluster = m_cluster_table[g_cluster_id];
                  int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
                  blocks_info[k + g_m + i].map2cluster = g_cluster_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_cluster.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_cluster.stripes.insert(stripe_id);
                  stripe_info.place2clusters.insert(g_cluster_id);
                }
              }
            }
          }
          Cluster &g_cluster = m_cluster_table[g_cluster_id];
          // place the global parity blocks to the selected cluster
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node(g_cluster_id, stripe_id);
            blocks_info[k + i].map2cluster = g_cluster_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_cluster.blocks.push_back(&blocks_info[k + i]);
            g_cluster.stripes.insert(stripe_id);
            stripe_info.place2clusters.insert(g_cluster_id);
          }
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int i = 0; i < m_num_of_Clusters; i++)
      {
        Cluster &t_cluster = m_cluster_table[i];
        if (int(t_cluster.blocks.size()) > 0)
        {
          std::cout << "Cluster " << i << ": ";
          for (auto it = t_cluster.blocks.begin(); it != t_cluster.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // randomly select a cluster
    int r_idx = rand_num(int(stripe_info.place2clusters.size()));
    int selected_cluster_id = *(std::next(stripe_info.place2clusters.begin(), r_idx));
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in cluster " << selected_cluster_id << " to encode and set!" << std::endl;
    }
    return selected_cluster_id;
  }

  void CoordinatorImpl::blocks_in_cluster(std::map<char, std::vector<ECProject::Block *>> &block_info, int cluster_id, int stripe_id)
  {
    std::vector<ECProject::Block *> tt, td, tl, tg;
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        tt.push_back(block);
        if (block->block_type == 'D')
        {
          td.push_back(block);
        }
        else if (block->block_type == 'L')
        {
          tl.push_back(block);
        }
        else
        {
          tg.push_back(block);
        }
      }
    }
    block_info['T'] = tt;
    block_info['D'] = td;
    block_info['L'] = tl;
    block_info['G'] = tg;
  }

  void CoordinatorImpl::find_max_group(int &max_group_id, int &max_group_num, int cluster_id, int stripe_id)
  {
    int group_cnt[5] = {0};
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      if ((*it)->map2stripe == stripe_id)
      {
        group_cnt[(*it)->map2group]++;
      }
    }
    for (int i = 0; i <= m_encode_parameters.l_localparityblock; i++)
    {
      if (group_cnt[i] > max_group_num)
      {
        max_group_id = i;
        max_group_num = group_cnt[i];
      }
    }
  }

  int CoordinatorImpl::count_block_num(char type, int cluster_id, int stripe_id, int group_id)
  {
    int cnt = 0;
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        if (group_id == -1)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
        else if (int(block->map2group) == group_id)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
      }
    }
    if (cnt == 0)
    {
      cluster.stripes.erase(stripe_id);
    }
    return cnt;
  }

  // find out if any specific type of block from the stripe exists in the cluster
  bool CoordinatorImpl::find_block(char type, int cluster_id, int stripe_id)
  {
    Cluster &cluster = m_cluster_table[cluster_id];
    std::vector<Block *>::iterator it;
    for (it = cluster.blocks.begin(); it != cluster.blocks.end(); it++)
    {
      if (stripe_id != -1 && int((*it)->map2stripe) == stripe_id && (*it)->block_type == type)
      {
        return true;
      }
      else if (stripe_id == -1 && (*it)->block_type == type)
      {
        return true;
      }
    }
    return false;
  }
} // namespace ECProject
