#include "cord_algorithm2.h"
#include "meta_definition.h"
#include <algorithm>
#include <iostream>
#include <limits>
#include <map>
#include <queue>
#include <set>
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

      struct FlowEdge
      {
        int to, rev, cap;
        int lid;
      };

      struct Dinic
      {
        int n, s, t;
        std::vector<std::vector<FlowEdge>> g;
        std::vector<int> level, it;
        Dinic(int n_, int s_, int t_) : n(n_), s(s_), t(t_), g(n_) {}

        void add_edge(int fr, int to, int cap, int lid)
        {
          int ga = static_cast<int>(g[fr].size());
          int gb = static_cast<int>(g[to].size());
          g[fr].push_back({to, gb, cap, lid});
          g[to].push_back({fr, ga, 0, -1});
        }

        bool bfs()
        {
          level.assign(n, -1);
          std::queue<int> q;
          level[s] = 0;
          q.push(s);
          while (!q.empty())
          {
            int v = q.front();
            q.pop();
            for (const FlowEdge &e : g[v])
            {
              if (e.cap > 0 && level[e.to] < 0)
              {
                level[e.to] = level[v] + 1;
                q.push(e.to);
              }
            }
          }
          return level[t] >= 0;
        }

        int dfs(int v, int f, std::vector<int> &itv)
        {
          if (v == t)
            return f;
          for (int &i = itv[v]; i < static_cast<int>(g[v].size()); ++i)
          {
            FlowEdge &e = g[v][i];
            if (e.cap > 0 && level[v] < level[e.to])
            {
              int d = dfs(e.to, std::min(f, e.cap), itv);
              if (d > 0)
              {
                e.cap -= d;
                g[e.to][e.rev].cap += d;
                return d;
              }
            }
          }
          return 0;
        }

        int maxflow()
        {
          int flow = 0, inf = 1e9;
          while (bfs())
          {
            it.assign(n, 0);
            int f;
            while ((f = dfs(s, inf, it)) > 0)
              flow += f;
          }
          return flow;
        }

        void collect_used_links(int C, std::vector<int> &out_link_ids)
        {
          for (int c = 0; c < C; ++c)
          {
            int L = 1 + c;
            for (const FlowEdge &e : g[L])
            {
              if (e.lid < 0)
                continue;
              FlowEdge const &rev = g[e.to][e.rev];
              if (rev.cap > 0)
                out_link_ids.push_back(e.lid);
            }
          }
        }
      };

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

      const int64_t parity_global_b = merged_delta_hull_span_bytes(block_intervals, D);
      if (parity_global_b <= 0)
        return out;

      for (int d : D)
      {
        if (d == collector_blk)
          continue;
        int64_t b = delta_bytes_for_block(block_intervals, d);
        if (b <= 0)
          continue;
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
        L.payload_bytes = parity_global_b;
        L.est_transfer_sec = transfer_sec(collector_cc, gc, parity_global_b, tp);
        L.group_index = gi;
        L.kind = TrainLinkKind::STAR_CENTER_TO_GLOBAL;
        L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
        L.parity_merge_data_block_ids = D;
        std::cout << "[CoRD-Alg2]   CTR_TO_GLOBAL: collector_blk" << collector_blk << "(c" << collector_cc
                  << ") --ΔP " << parity_global_b << "B --> global_blk" << gpar << "(c" << gc << ")\n";
        out.train_route.push_back(std::move(L));
      }

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
        const int64_t parity_local_b = merged_delta_hull_span_bytes(block_intervals, blocks);
        if (parity_local_b <= 0)
          continue;
        const int lc = block_cluster(stripe, Lb);
        const int rep = blocks.front();
        TrainLink L;
        L.src_block_id = rep;
        L.dst_block_id = Lb;
        L.src_cluster = rc;
        L.dst_cluster = lc;
        L.payload_bytes = parity_local_b;
        L.est_transfer_sec = transfer_sec(rc, lc, parity_local_b, tp);
        L.group_index = gi;
        L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
        L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
        L.parity_merge_data_block_ids = blocks;
        std::cout << "[CoRD-Alg2]   RACK_TO_LOCAL: rack=c" << rc << " rep_blk" << rep << " --ΔP " << parity_local_b
                  << "B --> local_blk" << Lb << "(c" << lc << ") group=" << gnum
                  << " merge_src=" << blocks.size() << " blocks\n";
        out.train_route.push_back(std::move(L));
      }

      const int C = cluster_num;
      const int S = 0;
      const int T = 2 + 2 * C;
      const int nV = T + 1;

      std::vector<int> remaining;
      remaining.reserve(out.train_route.size());
      std::cout << "[CoRD-Alg2] ===== Transfer scheduling: " << out.train_route.size() << " links =====\n";

      auto collector_star_data_ingress_done = [&](int group_idx, int collector_block_id) -> bool {
        for (size_t j = 0; j < out.train_route.size(); ++j)
        {
          const TrainLink &J = out.train_route[j];
          if (J.kind != TrainLinkKind::STAR_DATA_TO_CENTER)
            continue;
          if (J.group_index != group_idx || J.dst_block_id != collector_block_id)
            continue;
          if (remaining[j] > 0)
            return false;
        }
        return true;
      };

      auto link_needs_collector_ingress = [](const TrainLink &L) -> bool {
        if (L.kind == TrainLinkKind::STAR_CENTER_TO_GLOBAL)
          return true;
        if (L.kind == TrainLinkKind::STAR_CENTER_TO_LOCAL && L.src_block_id >= 0)
        {
          // rack-local：src 为 data block，不依赖 collector ingress
          return false;
        }
        return false;
      };

      auto link_eligible_for_step = [&](size_t i) -> bool {
        const TrainLink &L = out.train_route[i];
        if (link_needs_collector_ingress(L) &&
            !collector_star_data_ingress_done(L.group_index, out.collector_block_id))
          return false;
        return true;
      };

      for (size_t li = 0; li < out.train_route.size(); ++li)
        remaining.push_back(out.train_route[li].payload_bytes > 0 ? 1 : 0);

      int ts = 0;
      while (true)
      {
        bool any = false;
        for (int x : remaining)
        {
          if (x > 0)
          {
            any = true;
            break;
          }
        }
        if (!any)
          break;

        Dinic din(nV, S, T);
        for (int c = 0; c < C; ++c)
          din.add_edge(S, 1 + c, tp.enforce_one_send_one_recv_per_cluster ? 1 : C, -1);
        for (int c = 0; c < C; ++c)
          din.add_edge(1 + C + c, T, tp.enforce_one_send_one_recv_per_cluster ? 1 : C, -1);

        for (size_t i = 0; i < out.train_route.size(); ++i)
        {
          if (remaining[i] <= 0 || !link_eligible_for_step(i))
            continue;
          const TrainLink &L = out.train_route[i];
          if (L.src_cluster < 0 || L.dst_cluster < 0 || L.src_cluster >= C || L.dst_cluster >= C)
            continue;
          din.add_edge(1 + L.src_cluster, 1 + C + L.dst_cluster, 1, static_cast<int>(i));
        }

        din.maxflow();
        std::vector<int> used;
        din.collect_used_links(C, used);
        if (used.empty())
        {
          for (size_t i = 0; i < remaining.size(); ++i)
          {
            if (remaining[i] > 0 && link_eligible_for_step(i))
            {
              used.push_back(static_cast<int>(i));
              break;
            }
          }
        }

        TimeslotEntry te;
        te.timeslot = ts++;
        te.link_indices = std::move(used);
        for (int id : te.link_indices)
        {
          if (id >= 0 && id < static_cast<int>(remaining.size()) && remaining[id] > 0)
            remaining[id]--;
        }
        out.timeslot_schedule.push_back(std::move(te));
      }

      std::cout << "[CoRD-Alg2] ===== Transfer scheduling done: total_steps=" << ts << " =====\n";
      return out;
    }
  } // namespace cord_alg2
} // namespace ECProject
