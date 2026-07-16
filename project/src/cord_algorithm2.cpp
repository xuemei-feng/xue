#include "cord_algorithm2.h"
#include "cord_algorithm3.h"
#include "devcommon.h"
#include "meta_definition.h"
#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <limits>
#include <queue>
#include <set>
#include <sstream>

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
        const double lat =
            (src_c == dst_c) ? tp.same_cluster_latency_sec : tp.cross_cluster_latency_sec;
        double inv_bw_sec_per_byte = tp.inv_bw_sec_per_byte;
        if (src_c >= 0 && src_c < TransferParams::kMaxBwClusters && dst_c >= 0 &&
            dst_c < TransferParams::kMaxBwClusters)
        {
          const double bw_mbs = tp.bw_matrix_mb_per_sec[src_c][dst_c];
          if (bw_mbs > 0.0)
            inv_bw_sec_per_byte = 1.0 / (bw_mbs * 1024.0 * 1024.0);
        }
        return lat + static_cast<double>(bytes) * inv_bw_sec_per_byte;
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
      case TrainLinkKind::MST_FORWARD:
        return "MST_FORWARD";
      case TrainLinkKind::STAR_DATA_TO_LOCAL:
        return "STAR_DATA_TO_LOCAL";
      default:
        return "UNKNOWN";
      }
    }

    Algorithm2Result build_algorithm2(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        const std::vector<std::vector<int>> &U,
        int cluster_num,
        const TransferParams &tp)
    {
      Algorithm2Result out;
      const int k = stripe.k;
      const int r = stripe.r;
      if (cluster_num <= 0 || k <= 0)
        return out;

      int gi = 0;
      std::cout << "[CoRD-Alg2] ===== build_algorithm2 start: |U|=" << U.size() << " r=" << r
                << " k=" << k << " cluster_num=" << cluster_num << " =====\n";
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

        // 打印组内各 block 所属 cluster
        std::cout << "[CoRD-Alg2] --- group gi=" << gi << " |N|=" << N.size() << " blocks=[";
        for (size_t ni = 0; ni < N.size(); ++ni) {
          if (ni > 0) std::cout << " ";
          std::cout << N[ni] << "(c" << block_cluster(stripe, N[ni]) << ")";
        }
        std::cout << "] ---\n";

        if (static_cast<int>(N.size()) > 1)
        {
          const int64_t parity_global_b = merged_delta_hull_span_bytes(block_intervals, N);
          if (parity_global_b <= 0)
          {
            std::cout << "[CoRD-Alg2]   skip: parity_global_b=0\n";
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

          std::cout << "[CoRD-Alg2]   parity_global_hull=" << parity_global_b
                    << " bytes, local_groups_touched=[";
          for (size_t ti = 0; ti < groups_touched.size(); ++ti) {
            if (ti > 0) std::cout << " ";
            std::cout << groups_touched[ti];
          }
          std::cout << "]\n";

          auto emit_star_from_center = [&](int best_c, int cc_center) {
            std::cout << "[CoRD-Alg2]   >>> STAR center=global_blk" << best_c << " cluster=c" << cc_center << "\n";
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
              std::cout << "[CoRD-Alg2]     DATA_TO_CENTER: blk" << d << "(c" << dc << ") --ΔD "
                        << b << "B --> global_blk" << best_c << "(c" << cc_center
                        << ") est=" << L.est_transfer_sec << "s\n";
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
              std::cout << "[CoRD-Alg2]     CTR_TO_GLOBAL:  global_blk" << best_c << "(c" << cc_center
                        << ") --ΔP " << parity_global_b << "B --> global_blk" << g << "(c" << gc
                        << ") est=" << L.est_transfer_sec << "s merge_src=" << N.size() << " blocks\n";
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
              TrainLink L;
              L.src_block_id = best_c;
              L.dst_block_id = Lb;
              L.src_cluster = cc_center;
              L.dst_cluster = lc;
              L.payload_bytes = parity_local_b;
              L.est_transfer_sec = transfer_sec(cc_center, lc, parity_local_b, tp);
              L.group_index = gi;
              L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
              L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
              L.parity_merge_data_block_ids = blocks_same_local_group;
              std::cout << "[CoRD-Alg2]     CTR_TO_LOCAL:   global_blk" << best_c << "(c" << cc_center
                        << ") --ΔP " << parity_local_b << "B --> local_blk" << Lb << "(c" << lc
                        << ") group=" << gnum << " est=" << L.est_transfer_sec
                        << "s merge_src=" << blocks_same_local_group.size() << " blocks\n";
              out.train_route.push_back(std::move(L));
            }
          };

          bool fused_alg3 = false;
          if (static_cast<int>(N.size()) >= 3)
          {
            std::cout << "[CoRD-Alg2]   |N|=" << N.size() << " >= 3, attempting Algorithm3 PDP+DCP fusion...\n";
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
                // 打印 PDP 分组与 DCP 映射
                for (size_t j = 0; j < alg3.G.size(); ++j) {
                  const auto &grp = alg3.G[j];
                  int cidx = alg3.dcp.group_to_collector[j];
                  std::cout << "[CoRD-Alg2]   PDP group[" << j << "] → collector=global_blk" << (k + cidx)
                            << " span=" << grp.span_bytes << " sumΔ=" << grp.sum_delta_bytes
                            << " blocks=[";
                  for (size_t bi = 0; bi < grp.block_ids.size(); ++bi) {
                    if (bi > 0) std::cout << " ";
                    std::cout << grp.block_ids[bi];
                  }
                  std::cout << "]\n";
                }

                for (size_t j = 0; j < alg3.G.size(); ++j)
                {
                  const auto &grp = alg3.G[j];
                  if (grp.block_ids.empty())
                    continue;
                  int cidx = alg3.dcp.group_to_collector[j];
                  int col = k + cidx;
                  int cc = block_cluster(stripe, col);
                  std::cout << "[CoRD-Alg2]   >>> PDP group[" << j << "] → collector=global_blk" << col << " (c" << cc << ")\n";
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
                    std::cout << "[CoRD-Alg2]     DATA_TO_CENTER: blk" << d << "(c" << dc << ") --ΔD "
                              << b << "B --> global_blk" << col << "(c" << cc
                              << ") est=" << L.est_transfer_sec << "s\n";
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
                  std::cout << "[CoRD-Alg2]   >>> collector=global_blk" << col << " (c" << cc
                            << ") fan-out ΔP=" << parity_at_col << "B to other global + local parity blocks\n";
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
                    std::cout << "[CoRD-Alg2]     CTR_TO_GLOBAL:  global_blk" << col << "(c" << cc
                              << ") --ΔP " << parity_at_col << "B --> global_blk" << gpar << "(c" << gc
                              << ") est=" << L.est_transfer_sec << "s\n";
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
                    TrainLink L;
                    L.src_block_id = col;
                    L.dst_block_id = Lb;
                    L.src_cluster = cc;
                    L.dst_cluster = lc;
                    L.payload_bytes = parity_local_col;
                    L.est_transfer_sec = transfer_sec(cc, lc, parity_local_col, tp);
                    L.group_index = gi;
                    L.kind = TrainLinkKind::STAR_CENTER_TO_LOCAL;
                    L.delta_kind = CordDeltaPayloadKind::PARITY_DELTA;
                    L.parity_merge_data_block_ids = blocks_local;
                    std::cout << "[CoRD-Alg2]     CTR_TO_LOCAL:   global_blk" << col << "(c" << cc
                              << ") --ΔP " << parity_local_col << "B --> local_blk" << Lb << "(c" << lc
                              << ") group=" << gnum << " est=" << L.est_transfer_sec << "s\n";
                    out.train_route.push_back(std::move(L));
                  }
                }
                out.center_global_block_id = *used_collectors.begin();
              }
            }
          }

          if (!fused_alg3)
          {
            if (static_cast<int>(N.size()) >= 3)
              std::cout << "[CoRD-Alg2]   Algorithm3 not applied, fallback to single-center STAR\n";
            else
              std::cout << "[CoRD-Alg2]   |N|=2, using single-center STAR\n";
            int best_c = k;
            double best_cost = std::numeric_limits<double>::infinity();
            // 打印各候选中心的累计传输代价
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
              std::cout << "[CoRD-Alg2]   candidate center=global_blk" << cand << "(c" << cc
                        << ") sum_cost=" << sum << "s\n";
              if (sum < best_cost)
              {
                best_cost = sum;
                best_c = cand;
              }
            }
            out.center_global_block_id = best_c;
            int best_cc = block_cluster(stripe, best_c);
            std::cout << "[CoRD-Alg2]   chosen center=global_blk" << best_c << " c" << best_cc
                      << " cost=" << best_cost << "s\n";
            emit_star_from_center(best_c, best_cc);
          }
        }
        else
        {
          // ---------- 不相交集：MST ----------
          int d = N[0];
          int64_t bd = delta_bytes_for_block(block_intervals, d);
          if (bd <= 0)
          {
            std::cout << "[CoRD-Alg2]   skip MST: ΔD=0 for blk" << d << "\n";
            ++gi;
            continue;
          }
          int dc_src = block_cluster(stripe, d);
          const int Lb = local_parity_block_for_data_block(stripe, d);
          const int numV = 1 + r + (Lb >= 0 ? 1 : 0);
          std::vector<int> vid(static_cast<size_t>(numV));
          vid[0] = d;
          for (int j = 0; j < r; ++j)
            vid[static_cast<size_t>(1 + j)] = k + j;
          if (Lb >= 0)
            vid[static_cast<size_t>(1 + r)] = Lb;

          std::cout << "[CoRD-Alg2]   MST |N|=1 data_blk" << d << "(c" << dc_src
                    << ") ΔD=" << bd << "B  V={blk" << d;
          for (int j = 0; j < r; ++j)
            std::cout << ",G" << (k + j) << "(c" << block_cluster(stripe, k + j) << ")";
          if (Lb >= 0)
            std::cout << ",L" << Lb << "(c" << block_cluster(stripe, Lb) << ")";
          std::cout << "} |V|=" << numV << "\n";

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
          std::cout << "[CoRD-Alg2]   MST Kruskal edges selected:\n";
          for (const auto &e : edges)
          {
            if (dsu.unite(e.u, e.v)) {
              mst.push_back({e.u, e.v});
              std::cout << "[CoRD-Alg2]     " << vid[e.u] << " -- " << vid[e.v]
                        << " (idx " << e.u << "-" << e.v << ") w=" << e.w << "s\n";
            }
          }
          std::vector<std::pair<int, int>> directed;
          orient_mst_from_root(mst, 0, numV, &directed);
          std::cout << "[CoRD-Alg2]   MST directed (root=blk" << d << "):\n";
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
            std::cout << "[CoRD-Alg2]     MST_FORWARD: blk" << sb << "(c" << sc << ") --ΔD "
                      << bd << "B --> blk" << db << "(c" << dc
                      << ") est=" << L.est_transfer_sec << "s\n";
            out.train_route.push_back(std::move(L));
          }
        }
        ++gi;
      }

      // ---------- 时间步调度（每条链路一次性传完 payload；每步 cluster 容量 + 依赖约束） ----------
      const int C = cluster_num;
      const int S = 0;
      const int T = 2 + 2 * C;
      const int nV = T + 1;

      std::vector<int> remaining;
      remaining.reserve(out.train_route.size());
      std::cout << "[CoRD-Alg2] ===== Transfer scheduling: " << out.train_route.size()
                << " links (full payload per link) =====\n";
      for (size_t li = 0; li < out.train_route.size(); ++li) {
        const auto &L = out.train_route[li];
        remaining.push_back(L.payload_bytes > 0 ? 1 : 0);
        std::cout << "[CoRD-Alg2]   link[" << li << "] " << train_link_kind_name(L.kind)
                  << " blk" << L.src_block_id << "(c" << L.src_cluster << ")->blk" << L.dst_block_id
                  << "(c" << L.dst_cluster << ") payload=" << L.payload_bytes
                  << "B grp=" << L.group_index;
        if (L.delta_kind == CordDeltaPayloadKind::PARITY_DELTA)
          std::cout << " [depends on DATA_TO_CENTER ingressing to blk" << L.src_block_id << "]";
        std::cout << "\n";
      }

      int ts = 0;
      /** 收集器扇出校验增量前，同组内发往该 collector 的所有数据增量链路须已完成。 */
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
      /** MST 中继：同 origin 下，指向本链路 src 的入边须先完成。 */
      auto mst_predecessors_done = [&](size_t i) -> bool {
        const TrainLink &L = out.train_route[i];
        if (L.kind != TrainLinkKind::MST_FORWARD)
          return true;
        for (size_t j = 0; j < out.train_route.size(); ++j)
        {
          if (j == i)
            continue;
          const TrainLink &J = out.train_route[j];
          if (J.kind != TrainLinkKind::MST_FORWARD)
            continue;
          if (J.mst_origin_data_block != L.mst_origin_data_block)
            continue;
          if (J.dst_block_id != L.src_block_id)
            continue;
          if (remaining[j] > 0)
            return false;
        }
        return true;
      };
      auto link_eligible_for_step = [&](size_t i) -> bool {
        const TrainLink &L = out.train_route[i];
        if (L.group_index == kCordGlobalXorFinalGroupIndex)
        {
          for (size_t j = 0; j < out.train_route.size(); ++j)
          {
            if (j != i && remaining[j] > 0)
              return false;
          }
          return true;
        }
        if (L.kind == TrainLinkKind::STAR_CENTER_TO_GLOBAL || L.kind == TrainLinkKind::STAR_CENTER_TO_LOCAL)
        {
          if (!collector_star_data_ingress_done(L.group_index, L.src_block_id))
            return false;
        }
        return mst_predecessors_done(i);
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
          din.add_edge(S, 1 + c, tp.enforce_one_send_one_recv_per_cluster ? 1 : C, -1);
        for (int c = 0; c < C; ++c)
          din.add_edge(1 + C + c, T, tp.enforce_one_send_one_recv_per_cluster ? 1 : C, -1);

        // 打印本时间步候选链路
        std::cout << "[CoRD-Alg2] --- step " << ts << " candidates (pending & eligible):\n";
        for (size_t i = 0; i < out.train_route.size(); ++i)
        {
          if (remaining[i] <= 0)
            continue;
          bool eligible = link_eligible_for_step(i);
          const TrainLink &L = out.train_route[i];
          std::cout << "[CoRD-Alg2]   link[" << i << "] blk" << L.src_block_id << "->blk" << L.dst_block_id
                    << " c" << L.src_cluster << "->c" << L.dst_cluster
                    << " pending=" << remaining[i] << " eligible=" << (eligible ? "Y" : "N") << "\n";
          if (!eligible)
            continue;
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
            if (remaining[i] > 0 && link_eligible_for_step(i))
            {
              std::cout << "[CoRD-Alg2]   [deadlock-prevention] forcing link[" << i << "]\n";
              used.push_back(static_cast<int>(i));
              break;
            }
          }
        }
        std::cout << "[CoRD-Alg2]   step " << ts << " selected " << used.size() << " link(s): [";
        for (size_t ui = 0; ui < used.size(); ++ui) {
          if (ui > 0) std::cout << ", ";
          int li = used[ui];
          const auto &L = out.train_route[static_cast<size_t>(li)];
          std::cout << li << "(" << train_link_kind_name(L.kind)
                    << " c" << L.src_cluster << "→c" << L.dst_cluster << ")";
        }
        std::cout << "] — these links may run concurrently in this step\n";

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

    void schedule_train_route_timeslots(Algorithm2Result *out, int cluster_num, const TransferParams &tp)
    {
      if (out == nullptr)
        return;
      out->timeslot_schedule.clear();
      if (out->train_route.empty())
        return;

      const int C = cluster_num;
      const int S = 0;
      const int T = 2 + 2 * C;
      const int nV = T + 1;

      std::vector<int> remaining;
      remaining.reserve(out->train_route.size());
      for (const auto &L : out->train_route)
        remaining.push_back(L.payload_bytes > 0 ? 1 : 0);

      auto collector_star_data_ingress_done = [&](int group_idx, int collector_block_id) -> bool {
        for (size_t j = 0; j < out->train_route.size(); ++j)
        {
          const TrainLink &J = out->train_route[j];
          if (J.kind != TrainLinkKind::STAR_DATA_TO_CENTER)
            continue;
          if (J.group_index != group_idx || J.dst_block_id != collector_block_id)
            continue;
          if (remaining[j] > 0)
            return false;
        }
        return true;
      };

      auto mst_predecessors_done = [&](size_t i) -> bool {
        const TrainLink &L = out->train_route[i];
        if (L.kind != TrainLinkKind::MST_FORWARD)
          return true;
        for (size_t j = 0; j < out->train_route.size(); ++j)
        {
          if (j == i)
            continue;
          const TrainLink &J = out->train_route[j];
          if (J.kind != TrainLinkKind::MST_FORWARD)
            continue;
          if (J.mst_origin_data_block != L.mst_origin_data_block)
            continue;
          if (J.dst_block_id != L.src_block_id)
            continue;
          if (remaining[j] > 0)
            return false;
        }
        return true;
      };

      auto link_eligible_for_step = [&](size_t i) -> bool {
        const TrainLink &L = out->train_route[i];
        if (L.group_index == kCordGlobalXorFinalGroupIndex)
        {
          for (size_t j = 0; j < out->train_route.size(); ++j)
          {
            if (j != i && remaining[j] > 0)
              return false;
          }
          return true;
        }
        if (L.kind == TrainLinkKind::STAR_CENTER_TO_GLOBAL || L.kind == TrainLinkKind::STAR_CENTER_TO_LOCAL)
        {
          if (!collector_star_data_ingress_done(L.group_index, L.src_block_id))
            return false;
        }
        return mst_predecessors_done(i);
      };

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
        const int cap = tp.enforce_one_send_one_recv_per_cluster ? 1 : C;
        for (int c = 0; c < C; ++c)
          din.add_edge(S, 1 + c, cap, -1);
        for (int c = 0; c < C; ++c)
          din.add_edge(1 + C + c, T, cap, -1);

        for (size_t i = 0; i < out->train_route.size(); ++i)
        {
          if (remaining[i] <= 0 || !link_eligible_for_step(i))
            continue;
          const TrainLink &L = out->train_route[i];
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
        out->timeslot_schedule.push_back(std::move(te));
      }
    }

    void ensure_global_xor_final_timeslot_last(Algorithm2Result *out)
    {
      if (out == nullptr || out->timeslot_schedule.empty())
        return;
      int final_li = -1;
      for (size_t i = 0; i < out->train_route.size(); ++i)
      {
        if (out->train_route[i].group_index == kCordGlobalXorFinalGroupIndex)
        {
          final_li = static_cast<int>(i);
          break;
        }
      }
      if (final_li < 0)
        return;
      for (auto &te : out->timeslot_schedule)
      {
        auto &idx = te.link_indices;
        idx.erase(std::remove(idx.begin(), idx.end(), final_li), idx.end());
      }
      TimeslotEntry te;
      te.timeslot = out->timeslot_schedule.empty() ? 0 : out->timeslot_schedule.back().timeslot + 1;
      te.link_indices.push_back(final_li);
      out->timeslot_schedule.push_back(std::move(te));
    }

    bool load_bw_matrix_from_limitsame_file(const std::string &path, int cluster_num, TransferParams *tp)
    {
      if (tp == nullptr || cluster_num <= 0 || cluster_num > TransferParams::kMaxBwClusters)
        return false;
      for (int i = 0; i < TransferParams::kMaxBwClusters; ++i)
        for (int j = 0; j < TransferParams::kMaxBwClusters; ++j)
          tp->bw_matrix_mb_per_sec[i][j] = 0.0;

      std::ifstream ifs(path);
      if (!ifs)
        return false;
      std::string line;
      bool in_array = false;
      std::vector<double> vals;
      while (std::getline(ifs, line))
      {
        if (line.find("BW_MATRIX_MB_PER_SEC=(") != std::string::npos)
        {
          in_array = true;
          continue;
        }
        if (!in_array)
          continue;
        if (line.find(')') != std::string::npos)
          break;
        std::istringstream iss(line);
        double v = 0.0;
        while (iss >> v)
          vals.push_back(v);
      }
      if (static_cast<int>(vals.size()) < cluster_num * cluster_num)
      {
        std::cout << "[CoRD-Class] BW matrix parse failed: need " << (cluster_num * cluster_num) << " got "
                  << vals.size() << " from " << path << "\n";
        return false;
      }
      for (int i = 0; i < cluster_num; ++i)
        for (int j = 0; j < cluster_num; ++j)
          tp->bw_matrix_mb_per_sec[i][j] = vals[static_cast<size_t>(i * cluster_num + j)];
      if (cord_verbose_enabled())
        std::cout << "[CoRD-Class] Loaded BW matrix " << cluster_num << "x" << cluster_num << " from " << path << "\n";
      return true;
    }
  } // namespace cord_alg2
} // namespace ECProject
