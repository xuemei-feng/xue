#include "cord_algorithm2.h"
#include "cord_algorithm3.h"
#include "meta_definition.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <queue>
#include <set>

namespace ECProject
{
  namespace cord_alg2
  {
    // 合并多个数据块的更新区间，输出最小外包区间 [lo, hi) 的长度（中间可能有空洞）
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
      //返回一个数据块的更新区间总长度，比如数据块1的更新区间是[100,200)和[300,400)，则返回200-100+400-300=200
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

      int local_parity_block_for_data_block(const Stripe &stripe, int data_bid)
      {
        if (data_bid < 0 || data_bid >= stripe.k)
          return -1;
        const int gnum = stripe.blocks[data_bid]->map2group;
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

      struct DSU
      {
        std::vector<int> p;
        explicit DSU(int n) : p(n)
        {
          for (int i = 0; i < n; ++i)
            p[i] = i;
        }
        int find(int x)
        {
          return p[x] == x ? x : (p[x] = find(p[x]));
        }
        bool unite(int a, int b)
        {
          a = find(a);
          b = find(b);
          if (a == b)
            return false;
          p[a] = b;
          return true;
        }
      };

      struct KruskalEdge
      {
        int u, v;
        double w;
        bool operator<(KruskalEdge const &o) const { return w < o.w; }
      };

      // ---------- Dinic（S→L_i cap1，L_i→R_j 来自链路，R_j→T cap1）----------
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

        // 读出 L→R 上前向边（原 cap=1）上是否流过 1 单位：看反向边 cap
        void collect_used_links(int C, std::vector<int> &out_link_ids)
        {
          for (int c = 0; c < C; ++c)
          {
            int L = 1 + c;
            for (const FlowEdge &e : g[L])
            {
              if (e.lid < 0)
                continue;
              // 初始前向 cap=1，流 1 则前向 cap=0，反向 cap=1
              FlowEdge const &rev = g[e.to][e.rev];
              if (rev.cap > 0)
                out_link_ids.push_back(e.lid);
            }
          }
        }
      };

      void orient_mst_from_root(
          const std::vector<std::pair<int, int>> &mst_undirected,
          int root,
          int n_nodes,
          std::vector<std::pair<int, int>> *out_directed)
      {
        std::vector<std::vector<int>> adj(n_nodes);
        for (auto e : mst_undirected)
        {
          adj[e.first].push_back(e.second);
          adj[e.second].push_back(e.first);
        }
        std::vector<int> parent(n_nodes, -2);
        std::queue<int> q;
        parent[root] = -1;
        q.push(root);
        while (!q.empty())
        {
          int u = q.front();
          q.pop();
          for (int v : adj[u])
          {
            if (parent[v] == -2)
            {
              parent[v] = u;
              q.push(v);
            }
          }
        }
        for (int v = 0; v < n_nodes; ++v)
        {
          if (v == root)
            continue;
          int p = parent[v];
          if (p < 0)
            continue;
          // 由靠近根一侧指向外侧：p 更近根 => p -> v
          out_directed->push_back({p, v});
        }
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
        const TransferParams &tp,
        int slot_unit_bytes)
    {
      Algorithm2Result out;
      out.slot_unit_bytes = std::max(1, slot_unit_bytes);
      const int k = stripe.k;
      const int r = stripe.r;
      if (cluster_num <= 0 || k <= 0)
        return out;

      int gi = 0;
      for (const std::vector<int> &N : U)
      {
        if (N.empty())
        {
          ++gi;
          continue;
        }
        if (r <= 0)
        {
          ++gi;
          continue;
        }

        if (static_cast<int>(N.size()) > 1)
        {
          const int64_t parity_global_b = merged_delta_hull_span_bytes(block_intervals, N);
          if (parity_global_b <= 0)
          {
            ++gi;
            continue;
          }

          std::vector<int> groups_touched;
          for (int d : N)
          {
            int gnum = stripe.blocks[d]->map2group;
            if (std::find(groups_touched.begin(), groups_touched.end(), gnum) == groups_touched.end())
              groups_touched.push_back(gnum);
          }

          auto emit_star_from_center = [&](int best_c, int cc_center) {
            for (int d : N)
            {
              int64_t b = delta_bytes_for_block(block_intervals, d);
              if (b <= 0)
                continue;
              int dc = block_cluster(stripe, d);
              TrainLink L;
              L.src_block_id = d;
              L.dst_block_id = best_c;
              L.src_cluster = dc;
              L.dst_cluster = cc_center;
              L.payload_bytes = b;
              L.est_transfer_sec = transfer_sec(dc, cc_center, b, tp);
              L.group_index = gi;
              L.kind = TrainLinkKind::STAR_DATA_TO_CENTER;
              L.delta_kind = CordDeltaPayloadKind::DATA_DELTA;
              out.train_route.push_back(std::move(L));
            }
            for (int g = k; g < k + r; ++g)
            {
              if (g == best_c)
                continue;
              int gc = block_cluster(stripe, g);
              TrainLink L;
              L.src_block_id = best_c;
              L.dst_block_id = g;
              L.src_cluster = cc_center;
              L.dst_cluster = gc;
              L.payload_bytes = parity_global_b;
              L.est_transfer_sec = transfer_sec(cc_center, gc, parity_global_b, tp);
              L.group_index = gi;
              L.kind = TrainLinkKind::STAR_CENTER_TO_GLOBAL;
              L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
              L.parity_merge_data_block_ids.assign(N.begin(), N.end());
              out.train_route.push_back(std::move(L));
            }
            for (int gnum : groups_touched)
            {
              int Lb = -1;
              for (int i = k + r; i < stripe.n; ++i)
              {
                if (stripe.blocks[i]->map2group == gnum)
                {
                  Lb = i;
                  break;
                }
              }
              if (Lb < 0)
                continue;
              std::vector<int> blocks_same_local_group;
              for (int d : N)
              {
                if (stripe.blocks[d]->map2group == gnum)
                  blocks_same_local_group.push_back(d);
              }
              const int64_t parity_local_b = merged_delta_hull_span_bytes(block_intervals, blocks_same_local_group);
              if (parity_local_b <= 0)
                continue;
              int lc = block_cluster(stripe, Lb);

              // 优化：将同机架数据块拆出，由 proxy 在机架内直接 XOR 更新本地校验；
              // 跨机架数据块仍走 collector -> STAR_CENTER_TO_LOCAL。
              // 全局校验块不受影响（所有数据块 delta 仍发往 collector）。
              std::vector<int> in_rack_blocks, cross_rack_blocks;
              for (int d : blocks_same_local_group)
              {
                if (block_cluster(stripe, d) == lc)
                  in_rack_blocks.push_back(d);
                else
                  cross_rack_blocks.push_back(d);
              }
              for (int d : in_rack_blocks)
                out.local_parity_in_rack_data_blocks.insert(d);

              if (!cross_rack_blocks.empty())
              {
                const int64_t parity_local_b_cross =
                    merged_delta_hull_span_bytes(block_intervals, cross_rack_blocks);
                if (parity_local_b_cross > 0)
                {
                  TrainLink L;
                  L.src_block_id = best_c;
                  L.dst_block_id = Lb;
                  L.src_cluster = cc_center;
                  L.dst_cluster = lc;
                  L.payload_bytes = parity_local_b_cross;
                  L.est_transfer_sec = transfer_sec(cc_center, lc, parity_local_b_cross, tp);
                  L.group_index = gi;
                  L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
                  L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
                  L.parity_merge_data_block_ids = cross_rack_blocks;
                  out.train_route.push_back(std::move(L));
                }
              }
            }
          };

          bool fused_alg3 = false;
          if (static_cast<int>(N.size()) >= 3)
          {
            cord_alg3::Algorithm3Result alg3 =
                cord_alg3::build_algorithm3(stripe, block_intervals, N, cluster_num, tp);
            if (alg3.applied && alg3.G.size() == alg3.dcp.group_to_collector.size() && !alg3.G.empty())
            {
              std::set<int> used_collectors;
              bool assign_ok = true;
              for (size_t j = 0; j < alg3.G.size(); ++j)
              {
                int cidx = alg3.dcp.group_to_collector[j];
                if (cidx < 0 || cidx >= r)
                {
                  assign_ok = false;
                  break;
                }
                used_collectors.insert(k + cidx);
              }
              if (assign_ok && !used_collectors.empty())
              {
                fused_alg3 = true;
                std::cout << "[CoRD] Algorithm 2+3 fused |N|=" << N.size() << " P=" << alg3.G.size()
                          << " g_col=" << alg3.g << " collectors:";
                for (int col : used_collectors)
                  std::cout << " " << col;
                std::cout << "\n";

                for (size_t j = 0; j < alg3.G.size(); ++j)
                {
                  const auto &grp = alg3.G[j];
                  if (grp.block_ids.empty())
                    continue;
                  int cidx = alg3.dcp.group_to_collector[j];
                  int col = k + cidx;
                  int cc = block_cluster(stripe, col);
                  for (int d : grp.block_ids)
                  {
                    int64_t b = delta_bytes_for_block(block_intervals, d);
                    if (b <= 0)
                      continue;
                    int dc = block_cluster(stripe, d);
                    TrainLink L;
                    L.src_block_id = d;
                    L.dst_block_id = col;
                    L.src_cluster = dc;
                    L.dst_cluster = cc;
                    L.payload_bytes = b;
                    L.est_transfer_sec = transfer_sec(dc, cc, b, tp);
                    L.group_index = gi;
                    L.kind = TrainLinkKind::STAR_DATA_TO_CENTER;
                    L.delta_kind = CordDeltaPayloadKind::DATA_DELTA;
                    out.train_route.push_back(std::move(L));
                  }
                }

                for (int col : used_collectors)
                {
                  int cc = block_cluster(stripe, col);
                  std::vector<int> blocks_to_col;
                  for (size_t j = 0; j < alg3.G.size(); ++j)
                  {
                    if (k + alg3.dcp.group_to_collector[j] != col)
                      continue;
                    for (int bid : alg3.G[j].block_ids)
                      blocks_to_col.push_back(bid);
                  }
                  const int64_t parity_at_col =
                      merged_delta_hull_span_bytes(block_intervals, blocks_to_col);
                  if (parity_at_col <= 0)
                    continue;
                  for (int gpar = k; gpar < k + r; ++gpar)
                  {
                    if (gpar == col)
                      continue;
                    int gc = block_cluster(stripe, gpar);
                    TrainLink L;
                    L.src_block_id = col;
                    L.dst_block_id = gpar;
                    L.src_cluster = cc;
                    L.dst_cluster = gc;
                    L.payload_bytes = parity_at_col;
                    L.est_transfer_sec = transfer_sec(cc, gc, parity_at_col, tp);
                    L.group_index = gi;
                    L.kind = TrainLinkKind::STAR_CENTER_TO_GLOBAL;
                    L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
                    L.parity_merge_data_block_ids = blocks_to_col;
                    out.train_route.push_back(std::move(L));
                  }
                  for (int gnum : groups_touched)
                  {
                    int Lb = -1;
                    for (int i = k + r; i < stripe.n; ++i)
                    {
                      if (stripe.blocks[i]->map2group == gnum)
                      {
                        Lb = i;
                        break;
                      }
                    }
                    if (Lb < 0)
                      continue;
                    std::vector<int> blocks_local;
                    for (size_t j = 0; j < alg3.G.size(); ++j)
                    {
                      if (k + alg3.dcp.group_to_collector[j] != col)
                        continue;
                      for (int bid : alg3.G[j].block_ids)
                      {
                        if (stripe.blocks[bid]->map2group == gnum)
                          blocks_local.push_back(bid);
                      }
                    }
                    const int64_t parity_local_col =
                        merged_delta_hull_span_bytes(block_intervals, blocks_local);
                    if (parity_local_col <= 0)
                      continue;
                    int lc = block_cluster(stripe, Lb);
                    std::vector<int> in_rack_local, cross_rack_local;
                    for (int bid : blocks_local)
                    {
                      if (block_cluster(stripe, bid) == lc)
                        in_rack_local.push_back(bid);
                      else
                        cross_rack_local.push_back(bid);
                    }
                    for (int bid : in_rack_local)
                      out.local_parity_in_rack_data_blocks.insert(bid);

                    if (!cross_rack_local.empty())
                    {
                      const int64_t parity_local_col_cross =
                          merged_delta_hull_span_bytes(block_intervals, cross_rack_local);
                      if (parity_local_col_cross > 0)
                      {
                        TrainLink L;
                        L.src_block_id = col;
                        L.dst_block_id = Lb;
                        L.src_cluster = cc;
                        L.dst_cluster = lc;
                        L.payload_bytes = parity_local_col_cross;
                        L.est_transfer_sec = transfer_sec(cc, lc, parity_local_col_cross, tp);
                        L.group_index = gi;
                        L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
                        L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
                        L.parity_merge_data_block_ids = cross_rack_local;
                        out.train_route.push_back(std::move(L));
                      }
                    }
                  }
                }
                out.center_global_block_id = *used_collectors.begin();
              }
            }
          }

          if (!fused_alg3)
          {
            int best_c = k;
            double best_cost = std::numeric_limits<double>::infinity();
            for (int cand = k; cand < k + r; ++cand)
            {
              int cc = block_cluster(stripe, cand);
              if (cc < 0)
                continue;
              double sum = 0.0;
              for (int d : N)
              {
                int64_t b = delta_bytes_for_block(block_intervals, d);
                if (b <= 0)
                  continue;
                int dc = block_cluster(stripe, d);
                if (dc < 0)
                  continue;
                sum += transfer_sec(dc, cc, b, tp);
              }
              if (sum < best_cost)
              {
                best_cost = sum;
                best_c = cand;
              }
            }
            out.center_global_block_id = best_c;
            emit_star_from_center(best_c, block_cluster(stripe, best_c));
          }
        }
        else
        {
          // ---------- 不相交集：MST ----------
          int d = N[0];
          int64_t bd = delta_bytes_for_block(block_intervals, d);
          if (bd <= 0)
          {
            ++gi;
            continue;
          }
          const int Lb = local_parity_block_for_data_block(stripe, d);
          const int numV = 1 + r + (Lb >= 0 ? 1 : 0);
          std::vector<int> vid(static_cast<size_t>(numV));
          vid[0] = d;
          for (int j = 0; j < r; ++j)
            vid[static_cast<size_t>(1 + j)] = k + j;
          if (Lb >= 0)
            vid[static_cast<size_t>(1 + r)] = Lb;

          std::vector<KruskalEdge> edges;
          edges.reserve(static_cast<size_t>(numV * (numV - 1) / 2));
          for (int i = 0; i < numV; ++i)
          {
            for (int j = i + 1; j < numV; ++j)
            {
              int bi = vid[i], bj = vid[j];
              int ci = block_cluster(stripe, bi);
              int cj = block_cluster(stripe, bj);
              double w;
              if (i == 0 || j == 0)
              {
                int dcl = (i == 0) ? ci : cj;
                int gcl = (i == 0) ? cj : ci;
                w = transfer_sec(dcl, gcl, bd, tp);
              }
              else
              {
                w = transfer_sec(ci, cj, bd, tp);
              }
              edges.push_back({i, j, w});
            }
          }
          std::sort(edges.begin(), edges.end());
          DSU dsu(numV);
          std::vector<std::pair<int, int>> mst;
          for (const auto &e : edges)
          {
            if (dsu.unite(e.u, e.v))
              mst.push_back({e.u, e.v});
          }
          std::vector<std::pair<int, int>> directed;
          orient_mst_from_root(mst, 0, numV, &directed);
          for (auto pr : directed)
          {
            int sb = vid[pr.first];
            int db = vid[pr.second];
            int sc = block_cluster(stripe, sb);
            int dc = block_cluster(stripe, db);
            TrainLink L;
            L.src_block_id = sb;
            L.dst_block_id = db;
            L.src_cluster = sc;
            L.dst_cluster = dc;
            L.payload_bytes = bd;
            L.est_transfer_sec = transfer_sec(sc, dc, bd, tp);
            L.group_index = gi;
            L.kind = TrainLinkKind::MST_FORWARD;
            // |N|=1：最小生成树上每跳均传输同一数据块的数据增量 ΔD（非校验增量）
            L.delta_kind = CordDeltaPayloadKind::DATA_DELTA;
            L.mst_origin_data_block = d;
            out.train_route.push_back(std::move(L));
          }
        }
        ++gi;
      }

      // ---------- 最大流时隙调度 ----------
      const int C = cluster_num;
      const int S = 0;
      const int T = 2 + 2 * C;
      const int nV = T + 1;

      std::vector<int> remaining;
      remaining.reserve(out.train_route.size());
      for (const auto &L : out.train_route)
      {
        int slots = static_cast<int>(
            std::ceil(static_cast<double>(L.payload_bytes) / static_cast<double>(out.slot_unit_bytes)));
        remaining.push_back(std::max(1, slots));
      }

      int ts = 0;
      /** 收集器扇出校验增量前，同组内发往该 collector 的所有数据增量链路须已完成全部时隙（remaining==0）。 */
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
      auto link_eligible_for_slot = [&](size_t i) -> bool {
        const TrainLink &L = out.train_route[i];
        if (L.kind == TrainLinkKind::STAR_CENTER_TO_GLOBAL || L.kind == TrainLinkKind::STAR_CENTER_TO_LOCAL)
          return collector_star_data_ingress_done(L.group_index, L.src_block_id);
        return true;
      };

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
          din.add_edge(S, 1 + c, 1, -1);
        for (int c = 0; c < C; ++c)
          din.add_edge(1 + C + c, T, 1, -1);

        for (size_t i = 0; i < out.train_route.size(); ++i)
        {
          if (remaining[i] <= 0)
            continue;
          if (!link_eligible_for_slot(i))
            continue;
          const TrainLink &L = out.train_route[i];
          if (L.src_cluster < 0 || L.dst_cluster < 0 ||
              L.src_cluster >= C || L.dst_cluster >= C)
            continue;
          int Lu = 1 + L.src_cluster;
          int Rv = 1 + C + L.dst_cluster;
          din.add_edge(Lu, Rv, 1, static_cast<int>(i));
        }

        din.maxflow();
        std::vector<int> used;
        din.collect_used_links(C, used);
        if (used.empty())
        {
          // 无匹配：强制推进一条「依赖已满足」的剩余链路，避免死循环
          for (size_t i = 0; i < remaining.size(); ++i)
          {
            if (remaining[i] > 0 && link_eligible_for_slot(i))
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

      return out;
    }
  } // namespace cord_alg2
} // namespace ECProject
