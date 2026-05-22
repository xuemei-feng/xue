#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>
#include <chrono>
#include <limits>
#include <queue>
#include <set>
#include <cmath>
#include <stdexcept>
#include <iomanip>
#include <sstream>
#include <numeric>
#include <algorithm>

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
    bool is_azure_like_code(const std::string &code_type) // 辅助函数：判断是否为 Azure或xue类型的编码
    {
      return code_type == "AzureLRC" || code_type == "XueLRC";
    }

    int parse_group_id_from_cluster_append_key(const std::string &key)
    {
      const size_t us = key.find('_');
      if (us == std::string::npos)
      {
        return 0;
      }
      const size_t class_pos = key.find("_class", us + 1);
      if (class_pos != std::string::npos)
      {
        return std::stoi(key.substr(us + 1, class_pos - us - 1));
      }
      const size_t cpos = key.find('c', us + 1);
      size_t end = key.find('#', us + 1);
      if (end == std::string::npos)
      {
        end = key.size();
      }
      if (cpos != std::string::npos && cpos < end)
      {
        return std::stoi(key.substr(us + 1, cpos - us - 1));
      }
      return std::stoi(key.substr(us + 1, end - us - 1));
    }

    void fill_reply_from_append_plans(CoordinatorImpl *self,
                                      const std::vector<proxy_proto::AppendStripeDataPlacement> &plans,
                                      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
    {
      size_t sum_append_size = 0;
      for (const auto &plan : plans)
      {
        proxyIPPort->add_append_keys(plan.key());
        proxyIPPort->add_proxyips(self->m_cluster_table[plan.cluster_id()].proxy_ip);
        proxyIPPort->add_proxyports(self->m_cluster_table[plan.cluster_id()].proxy_port +
                                    ECProject::PROXY_PORT_SHIFT);
        proxyIPPort->add_cluster_slice_sizes(plan.append_size());
        proxyIPPort->add_group_ids(parse_group_id_from_cluster_append_key(plan.key()));
        sum_append_size += plan.append_size();
      }
      proxyIPPort->set_sum_append_size(sum_append_size);
    }

    std::vector<proxy_proto::AppendStripeDataPlacement> merge_append_plans_by_cluster(
        std::vector<proxy_proto::AppendStripeDataPlacement> plans)
    {
      struct BlockEntry
      {
        int block_id = 0;
        std::string datanode_ip;
        int datanode_port = 0;
        std::string block_key;
        int block_cluster = 0;
        int offset = 0;
        int size = 0;
        bool counts_tcp = false;
      };
      std::map<int, proxy_proto::AppendStripeDataPlacement> base_plan;
      std::map<int, std::vector<BlockEntry>> blocks_per_cluster;
      for (auto &plan : plans)
      {
        const int cluster_id = plan.cluster_id();
        if (base_plan.find(cluster_id) == base_plan.end())
        {
          base_plan[cluster_id] = plan;
          base_plan[cluster_id].clear_datanodeip();
          base_plan[cluster_id].clear_datanodeport();
          base_plan[cluster_id].clear_blockkeys();
          base_plan[cluster_id].clear_blockids();
          base_plan[cluster_id].clear_block_cluster_ids();
          base_plan[cluster_id].clear_offsets();
          base_plan[cluster_id].clear_sizes();
        }
        const int tcp_n =
            plan.xue_tcp_slice_count() > 0 ? plan.xue_tcp_slice_count() : plan.blockids_size();
        for (int j = 0; j < plan.blockids_size(); ++j)
        {
          BlockEntry e;
          e.block_id = plan.blockids(j);
          e.datanode_ip = plan.datanodeip(j);
          e.datanode_port = plan.datanodeport(j);
          e.block_key = plan.blockkeys(j);
          e.block_cluster =
              (j < plan.block_cluster_ids_size()) ? plan.block_cluster_ids(j) : cluster_id;
          e.offset = plan.offsets(j);
          e.size = plan.sizes(j);
          e.counts_tcp = (j < tcp_n);
          blocks_per_cluster[cluster_id].push_back(std::move(e));
        }
      }
      std::vector<proxy_proto::AppendStripeDataPlacement> merged;
      merged.reserve(blocks_per_cluster.size());
      for (auto &kv : blocks_per_cluster)
      {
        const int cluster_id = kv.first;
        auto &entries = kv.second;
        std::sort(entries.begin(), entries.end(),
                  [](const BlockEntry &a, const BlockEntry &b) { return a.block_id < b.block_id; });
        proxy_proto::AppendStripeDataPlacement out = base_plan[cluster_id];
        out.clear_datanodeip();
        out.clear_datanodeport();
        out.clear_blockkeys();
        out.clear_blockids();
        out.clear_block_cluster_ids();
        out.clear_offsets();
        out.clear_sizes();
        std::vector<int> tcp_block_ids;
        std::vector<int> meta_block_ids;
        tcp_block_ids.reserve(entries.size());
        meta_block_ids.reserve(entries.size());
        int tcp_slices = 0;
        size_t append_size = 0;
        for (const auto &e : entries)
        {
          if (e.counts_tcp)
          {
            tcp_block_ids.push_back(e.block_id);
          }
          else
          {
            meta_block_ids.push_back(e.block_id);
          }
          out.add_datanodeip(e.datanode_ip);
          out.add_datanodeport(e.datanode_port);
          out.add_blockkeys(e.block_key);
          out.add_blockids(e.block_id);
          out.add_block_cluster_ids(e.block_cluster);
          out.add_offsets(e.offset);
          out.add_sizes(e.size);
          if (e.counts_tcp)
          {
            ++tcp_slices;
            append_size += static_cast<size_t>(e.size);
          }
        }
        out.set_cluster_id(cluster_id);
        out.set_key(ToolBox::getInstance()->gen_append_key_cluster_plan(
            out.stripe_id(), -1, cluster_id, tcp_block_ids, meta_block_ids));
        out.set_append_size(append_size);
        out.set_xue_tcp_slice_count(tcp_slices);
        merged.push_back(std::move(out));
      }
      return merged;
    }

    int parse_xue_class_subgroup_from_plan_key(const std::string &key)
    {
      const size_t class_pos = key.find("_class");
      if (class_pos == std::string::npos)
      {
        return 0;
      }
      const size_t num_begin = class_pos + 6;
      const size_t cpos = key.find('c', num_begin);
      if (cpos == std::string::npos || cpos <= num_begin)
      {
        return 0;
      }
      return std::stoi(key.substr(num_begin, cpos - num_begin));
    }

    std::vector<proxy_proto::AppendStripeDataPlacement> split_placement_by_map2cluster(
        const proxy_proto::AppendStripeDataPlacement &plan, int group_id)
    {
      std::map<int, std::vector<int>> indices_by_cluster;
      const int n = plan.blockids_size();
      for (int j = 0; j < n; ++j)
      {
        int cl = (j < plan.block_cluster_ids_size()) ? plan.block_cluster_ids(j) : plan.cluster_id();
        indices_by_cluster[cl].push_back(j);
      }
      std::vector<proxy_proto::AppendStripeDataPlacement> out;
      out.reserve(indices_by_cluster.size());
      const int tcp_n = plan.xue_tcp_slice_count() > 0 ? plan.xue_tcp_slice_count() : n;
      for (const auto &kv : indices_by_cluster)
      {
        const int cluster_id = kv.first;
        std::vector<int> idxs = kv.second;
        // 含 client TCP 的 XUE_UPDATE 子 plan 需携带全部 parity 元数据（跨 cluster），
        // 供 proxy 链式转发/编码；仅按 map2cluster 拆分或只挂 global cluster 会漏块（XueLRC 尤甚）。
        const auto attach_all_parity_meta_to_tcp_subplan = [&]() {
          bool sub_has_tcp = false;
          for (int j : idxs)
          {
            if (j < tcp_n)
            {
              sub_has_tcp = true;
              break;
            }
          }
          if (!sub_has_tcp || plan.append_mode() != "XUE_UPDATE")
          {
            return;
          }
          for (int j = 0; j < n; ++j)
          {
            if (j < tcp_n)
            {
              continue;
            }
            if (std::find(idxs.begin(), idxs.end(), j) == idxs.end())
            {
              idxs.push_back(j);
            }
          }
          std::sort(idxs.begin(), idxs.end(),
                    [&plan](int a, int b) { return plan.blockids(a) < plan.blockids(b); });
        };
        if (plan.xue_class1_relay_path() && cluster_id == plan.cluster_id())
        {
          attach_all_parity_meta_to_tcp_subplan();
        }
        // 第2类：data/global 同 cluster 子 plan 需携带其它 cluster 的 local parity 元数据
        else if (plan.append_mode() == "XUE_UPDATE" && !plan.xue_class1_relay_path() &&
                 plan.xue_compute_global_parity() && plan.xue_global_parity_cluster_id() >= 0 &&
                 cluster_id == plan.xue_global_parity_cluster_id())
        {
          for (int j = 0; j < n; ++j)
          {
            if (j < tcp_n)
            {
              continue;
            }
            const int bcl =
                (j < plan.block_cluster_ids_size()) ? plan.block_cluster_ids(j) : -1;
            if (bcl >= 0 && bcl != cluster_id &&
                std::find(idxs.begin(), idxs.end(), j) == idxs.end())
            {
              idxs.push_back(j);
            }
          }
          std::sort(idxs.begin(), idxs.end(),
                    [&plan](int a, int b) { return plan.blockids(a) < plan.blockids(b); });
        }
        else if (plan.append_mode() == "XUE_UPDATE" && !plan.xue_class1_relay_path())
        {
          attach_all_parity_meta_to_tcp_subplan();
        }
        std::vector<int> tcp_block_ids;
        std::vector<int> meta_block_ids;
        tcp_block_ids.reserve(idxs.size());
        meta_block_ids.reserve(idxs.size());
        for (int j : idxs)
        {
          if (j < tcp_n)
          {
            tcp_block_ids.push_back(plan.blockids(j));
          }
          else
          {
            meta_block_ids.push_back(plan.blockids(j));
          }
        }
        proxy_proto::AppendStripeDataPlacement sub = plan;
        sub.clear_datanodeip();
        sub.clear_datanodeport();
        sub.clear_blockkeys();
        sub.clear_blockids();
        sub.clear_block_cluster_ids();
        sub.clear_offsets();
        sub.clear_sizes();
        sub.set_cluster_id(cluster_id);
        sub.set_key(ToolBox::getInstance()->gen_append_key_cluster_plan(
            plan.stripe_id(), group_id, cluster_id, tcp_block_ids, meta_block_ids,
            parse_xue_class_subgroup_from_plan_key(plan.key())));
        int sub_tcp = 0;
        size_t sub_append = 0;
        for (int j : idxs)
        {
          sub.add_datanodeip(plan.datanodeip(j));
          sub.add_datanodeport(plan.datanodeport(j));
          sub.add_blockkeys(plan.blockkeys(j));
          sub.add_blockids(plan.blockids(j));
          sub.add_block_cluster_ids(plan.block_cluster_ids(j));
          sub.add_offsets(plan.offsets(j));
          sub.add_sizes(plan.sizes(j));
          if (j < tcp_n)
          {
            ++sub_tcp;
            sub_append += plan.sizes(j);
          }
        }
        sub.set_xue_tcp_slice_count(sub_tcp);
        sub.set_append_size(sub_append);
        // 第2类：client TCP 落在 data/global 同 cluster 的子 plan 上，由该 proxy 计算 global parity
        if (plan.append_mode() == "XUE_UPDATE" && plan.xue_global_parity_cluster_id() >= 0 &&
            cluster_id == plan.xue_global_parity_cluster_id())
        {
          sub.set_xue_compute_global_parity(true);
        }
        if (sub_append <= 0 && plan.append_mode() != "XUE_UPDATE")
        {
          continue;
        }
        if (sub_append <= 0 && plan.append_mode() == "XUE_UPDATE")
        {
          continue;
        }
        out.push_back(std::move(sub));
      }
      return out;
    }
      // 强类型枚举：定义数据更新的分类
    enum class DataUpdateClass
    {
      kLocalParitySameCluster,  // 第1类：数据块与本地校验块同cluster
      kGlobalParitySameCluster, // 第2类：数据块与全局校验块同cluster
      kDataOnlyCluster          // 第3类：仅与数据块同cluster
    };

    struct DataSliceUpdate
    {
      int block_id = -1; // 数据块ID
      int group_id = -1; // 组ID
      int offset = 0; // 偏移量 ：数据块中的偏移量
      int size = 0; // 大小 ：数据块中的大小
      int data_cluster = -1; // 数据集群 ：数据块所在的集群
      int local_parity_cluster = -1; // 本地校验块集群 ：本地校验块所在的集群
      int global_parity_cluster = -1; // 全局校验块集群 ：全局校验块所在的集群
      DataUpdateClass klass = DataUpdateClass::kDataOnlyCluster; // 数据更新分类 ：数据更新分类
    };

    struct TransferStep  // 传输步骤
    {
      int from_cluster = -1; // 从哪个集群
      int to_cluster = -1; // 到哪个集群
      std::string payload; // data_delta / parity_delta ：数据/校验块的更新内容
      bool depends_on_prev = false;   // 是否依赖前一个步骤
      std::string path_desc;  //从哪个集群的哪类块更新内容传到哪个集群的哪类块
      double transfer_size = 1.0; // 传输大小（字节）
    };

    struct TransferPlanDecision  // 传输计划决策
    {
      std::vector<int> block_ids; // 数据块ID列表
      std::vector<TransferStep> steps; // 传输步骤列表 ：传输步骤列表
      std::string reason; // 决策原因 ：决策原因
      bool has_direct_fallback = false; // 是否 有 直接备选 ：是否有直接备选
      int direct_fallback_dst = -1; // 直接备选的目标集群 ：直接备选的目标集群
      int hot_cluster = -1; // 热点机架：该决策对应的全局校验所在集群
    };

    struct ScheduledTask  // 调度任务
    {
      int task_id = -1;   // 任务ID
      int decision_id = -1; // 决策ID
      int from_cluster = -1; // 从哪个集群
      int to_cluster = -1; // 到哪个集群
      std::string payload; // 更新内容
      double duration = 0.0; // 持续时间
      double start_time = 0.0; // 开始时间
      double end_time = 0.0; // 结束时间
      std::string path_desc; // 路径描述
    };

    struct Class1RelayRoute
    {
      bool enabled = false;
      int relay_cluster = -1;
      int global_parity_cluster = -1;
    };

    struct XueUpdateResult  // Xue更新结果
    {
      std::map<int, int> group_to_ingress_cluster; // 组到入口集群的映射
      std::map<int, Class1RelayRoute> group_to_class1_relay; // 第1类选中继时的两跳路由
      std::vector<TransferPlanDecision> route_decisions; //所有路径决策
      std::vector<ScheduledTask> scheduled_tasks; // 所有调度任务
    };

    struct RunningTask  // 运行任务
    {
      double end_time = 0.0; // 结束时间
      int task_id = -1; // 任务ID
      bool operator>(const RunningTask &other) const// 运算符重载：用于比较两个任务哪个先结束
      // 这在优先队列（Priority Queue）中非常有用，可以按结束时间排序
      {
        return end_time > other.end_time;  // 如果当前任务的结束时间大于另一个任务的结束时间，则当前任务先结束
      }
    };

    double estimate_bandwidth_between_clusters(int from_cluster, int to_cluster); // 估计两个集群之间的带宽

    inline std::string fmt_cluster_id(int c) // 格式化集群ID
    {
      return "cluster-" + std::to_string(c); // 返回集群ID
    }

    inline std::string fmt_data_update_range(const DataSliceUpdate &u) // 格式化数据更新区间
    {
      const int end_exclusive = u.offset + u.size; // 结束位置(开区间)
      return "数据块" + std::to_string(u.block_id) + "更新区间[" + std::to_string(u.offset) + "," + std::to_string(end_exclusive) + ")"; // 返回数据更新区间
    }

    // 同一数据块多切片：合并描述与总传输量（用于 class1/class2 及 xue_update_sparse 单决策批量步骤）
    inline std::string fmt_block_multi_slice_ranges(int block_id, const std::vector<DataSliceUpdate> &slices)
    {
      std::vector<std::pair<int, int>> rng;
      rng.reserve(slices.size());
      for (const auto &u : slices)
      {
        rng.push_back({u.offset, u.offset + u.size});
      }
      std::sort(rng.begin(), rng.end());
      std::string out = "数据块" + std::to_string(block_id) + "更新区间";
      for (size_t i = 0; i < rng.size(); ++i)
      {
        if (i > 0)
        {
          out += ",";
        }
        out += "[" + std::to_string(rng[i].first) + "," + std::to_string(rng[i].second) + ")";
      }
      return out;
    }

    inline double sum_slice_transfer_bytes(const std::vector<DataSliceUpdate> &slices)
    {
      double t = 0.0;
      for (const auto &u : slices)
      {
        t += static_cast<double>(u.size);
      }
      return std::max(1.0, t);
    }

    inline std::string fmt_global_parity_block(int block_id)  // 格式化全局校验块
    {
      return "全局校验块(block_id=" + std::to_string(block_id) + ")"; // 返回全局校验块
    }

    inline std::string fmt_local_parity_block(int block_id)  // 格式化本地校验块
    {
      return "本地校验块(block_id=" + std::to_string(block_id) + ")"; // 返回本地校验块
    }

    inline std::string fmt_sim_time(double t)
    {
      std::ostringstream os;
      os << std::fixed << std::setprecision(3) << t;
      return os.str();
    }

    inline std::string payload_content_cn(const std::string &payload)
    {
      if (payload == "data_delta")
      {
        return "数据增量(data_delta)";
      }
      if (payload == "parity_delta")
      {
        return "校验增量(parity_delta)";
      }
      return "载荷(" + payload + ")";
    }

    void log_append_route_decisions(const std::vector<TransferPlanDecision> &decisions)
    {
      (void)decisions;
      return; // unfinished function
    }

    void log_append_schedule_visual(const std::vector<ScheduledTask> &schedule)
    {
      // debug
      std::cout << "[log_append_schedule_visual] schedule:";
      for (const auto &t : schedule)
      {
        std::cout << " " << t.task_id << " " << t.from_cluster << " " << t.to_cluster << " " << t.payload << " " << t.path_desc << " " << t.start_time << " " << t.end_time << " " << t.duration << std::endl;
        std::cout << "  从 t=" << fmt_sim_time(t.start_time) << " 到 t=" << fmt_sim_time(t.end_time)
                  << "，传输 " << payload_content_cn(t.payload)
                  << "，从 " << fmt_cluster_id(t.from_cluster)
                  << " 到 " << fmt_cluster_id(t.to_cluster)
                  << "，内容: " << t.path_desc << std::endl;
      }
      // debug end
      if (schedule.empty())
      {
        std::cout << "[XUE_UPDATE_TRANSMISSION] (无传输任务)\n";
        return;
      }
      const int n = static_cast<int>(schedule.size());
      std::cout << "[XUE_UPDATE_TRANSMISSION] 共 " << n
                << " 条传输（时间为调度仿真相对时刻 t，与 duration 同单位）\n";

      std::vector<int> order(n);
      std::iota(order.begin(), order.end(), 0);
      std::sort(order.begin(), order.end(), [&](int a, int b) {
        if (schedule[a].start_time != schedule[b].start_time)
        {
          return schedule[a].start_time < schedule[b].start_time;
        }
        if (schedule[a].end_time != schedule[b].end_time)
        {
          return schedule[a].end_time < schedule[b].end_time;
        }
        return schedule[a].task_id < schedule[b].task_id;
      });

      for (int ii : order)
      {
        const auto &t = schedule[ii];
        std::cout << "  从 t=" << fmt_sim_time(t.start_time) << " 到 t=" << fmt_sim_time(t.end_time)
                  << "，传输 " << payload_content_cn(t.payload)
                  << "，从 " << fmt_cluster_id(t.from_cluster)
                  << " 到 " << fmt_cluster_id(t.to_cluster)
                  << "，内容: " << t.path_desc << std::endl;
      }
      return;
    }

    std::vector<ScheduledTask> schedule_transfer_steps(const std::vector<TransferPlanDecision> &decisions) // 调度传输步骤
    {
      std::vector<ScheduledTask> scheduled; // 调度任务列表
      if (decisions.empty())
      {
        return scheduled; // 如果决策列表为空，则返回空调度任务列表
      }

      struct RawTask
      {
        int decision_id = -1;
        int from_cluster = -1;
        int to_cluster = -1;
        std::string payload;
        double duration = 0.0;
        int alt_group = -1;
        int alt_kind = 0;
        std::string path_desc;
        double transfer_size = 1.0;
        int hot_cluster = -1;
      };

      std::vector<RawTask> tasks;
      std::vector<std::vector<int>> succ;
      std::vector<int> remaining_pred;
      auto add_task = [&](const RawTask &t) -> int {
        int id = static_cast<int>(tasks.size());
        tasks.push_back(t);
        succ.emplace_back();
        remaining_pred.push_back(0);
        return id;
      };

      std::vector<std::vector<int>> alt_group_tasks;

      // 构建任务图
      for (int d_id = 0; d_id < static_cast<int>(decisions.size()); d_id++)
      {
        const auto &d = decisions[d_id];
        int alt_group_id = -1;
        if (d.has_direct_fallback)
        {
          alt_group_id = static_cast<int>(alt_group_tasks.size());
          alt_group_tasks.push_back({});
        }
        int prev_task_id = -1;
        for (const auto &s : d.steps)
        {
          double bw = estimate_bandwidth_between_clusters(s.from_cluster, s.to_cluster);
          const double sz = std::max(1.0, s.transfer_size);
          double duration = (bw > 1e-6) ? (sz / bw) : 1e6;
          int alt_kind = 0;
          if (d.has_direct_fallback)
          {
            alt_kind = (prev_task_id < 0) ? 1 : 2;
          }
          int cur_id = add_task({d_id, s.from_cluster, s.to_cluster, s.payload, duration, alt_group_id, alt_kind, s.path_desc, sz, d.hot_cluster});
          if (alt_group_id >= 0)
          {
            alt_group_tasks[alt_group_id].push_back(cur_id);
          }
          if (s.depends_on_prev && prev_task_id >= 0)
          {
            succ[prev_task_id].push_back(cur_id);
            remaining_pred[cur_id]++;
          }
          prev_task_id = cur_id;
        }
        // 生成直传备选方案
        if (d.has_direct_fallback && !d.steps.empty() && d.direct_fallback_dst >= 0)
        {
          int src = d.steps[0].from_cluster;
          int dst = d.direct_fallback_dst;
          double bw = estimate_bandwidth_between_clusters(src, dst);
          const double sz = std::max(1.0, d.steps[0].transfer_size);
          double duration = (bw > 1e-6) ? (sz / bw) : 1e6;
          int direct_id = add_task({d_id, src, dst, "data_delta", duration, alt_group_id, 3,
                                    "直传备选 " + fmt_cluster_id(src) + " -> " + fmt_cluster_id(dst) + " (与中转路径二选一)", sz, d.hot_cluster});
          alt_group_tasks[alt_group_id].push_back(direct_id);
        }
      }

      const int n = static_cast<int>(tasks.size());
      if (n == 0)
      {
        return scheduled;
      }

      // 计算 dp_after（关键路径后继长度）
      std::vector<int> indeg = remaining_pred;
      std::queue<int> q;
      for (int i = 0; i < n; i++)
      {
        if (indeg[i] == 0)
          q.push(i);
      }
      std::vector<int> topo;
      while (!q.empty())
      {
        int u = q.front();
        q.pop();
        topo.push_back(u);
        for (int v : succ[u])
        {
          if (--indeg[v] == 0)
            q.push(v);
        }
      }
      std::vector<double> dp_after(n, 0.0);
      for (int i = static_cast<int>(topo.size()) - 1; i >= 0; i--)
      {
        int u = topo[i];
        double best = 0.0;
        for (int v : succ[u])
        {
          best = std::max(best, tasks[v].duration + dp_after[v]);
        }
        dp_after[u] = best;
      }

      std::map<int, double> best_to_hot_by_decision;
      for (int i = 0; i < n; i++)
      {
        if (tasks[i].hot_cluster >= 0 && tasks[i].to_cluster == tasks[i].hot_cluster)
        {
          const double bw = estimate_bandwidth_between_clusters(tasks[i].from_cluster, tasks[i].hot_cluster);
          auto it = best_to_hot_by_decision.find(tasks[i].decision_id);
          if (it == best_to_hot_by_decision.end())
          {
            best_to_hot_by_decision[tasks[i].decision_id] = bw;
          }
          else
          {
            it->second = std::max(it->second, bw);
          }
        }
      }

      std::vector<double> score(n, 0.0);
      for (int i = 0; i < n; i++)
      {
        double hot_bonus = 0.0;
        auto hit = best_to_hot_by_decision.find(tasks[i].decision_id);
        if (tasks[i].hot_cluster >= 0 &&
            tasks[i].to_cluster == tasks[i].hot_cluster &&
            hit != best_to_hot_by_decision.end() &&
            hit->second > 0.0)
        {
          const double bw = estimate_bandwidth_between_clusters(tasks[i].from_cluster, tasks[i].hot_cluster);
          if (std::abs(bw - hit->second) < 1e-9)
            hot_bonus = 1.0;
        }
        score[i] = 100.0 * dp_after[i] + 10.0 * tasks[i].duration + hot_bonus;
      }

      std::set<int> ready_set;
      for (int i = 0; i < n; i++)
      {
        if (remaining_pred[i] == 0)
          ready_set.insert(i);
      }
      std::vector<double> send_next(64, 0.0), recv_next(64, 0.0);
      std::vector<bool> finished(n, false);
      std::vector<bool> canceled(n, false);
      std::vector<bool> alt_group_resolved(alt_group_tasks.size(), false);
      int finished_cnt = 0;
      double current_t = 0.0;
      std::priority_queue<RunningTask, std::vector<RunningTask>, std::greater<RunningTask>> running;

      // ====== 调度循环开始 ======
      while (finished_cnt < n)
      {
        // 先清理：互斥组已决议后，其余同组任务不应继续留在 ready_set 中等待调度。
        // 若不清理，可能出现 candidates 非空但 selected 为空，且 next_t==current_t 的活锁。
        std::vector<int> stale_ready_tasks;
        for (int tid : ready_set)
        {
          const int g = tasks[tid].alt_group;
          if (g >= 0 && g < static_cast<int>(alt_group_resolved.size()) && alt_group_resolved[g])
          {
            // alt_group 已决议后，只清理互斥入口任务；
            // 不能清理已选链路上的后续任务（alt_kind=2），否则会吞掉“中继第二跳”。
            const int kind = tasks[tid].alt_kind;
            if (kind == 1 || kind == 3)
            {
              stale_ready_tasks.push_back(tid);
            }
          }
        }
        for (int tid : stale_ready_tasks)
        {
          ready_set.erase(tid);
          if (!finished[tid])
          {
            canceled[tid] = true;
            finished[tid] = true;
            finished_cnt++;
            std::cout << "[debug] purge stale alt_task from ready_set: " << tid << std::endl;
          }
        }
        if (finished_cnt >= n)
        {
          break;
        }

        // ==== DEBUG: 输出当前ready_set和资源状态 ====
        std::cout << "[debug] current_t=" << current_t << ", finished_cnt=" << finished_cnt
                  << ", running-tasks=" << running.size() << ", ready_set={";
        for (int tid : ready_set) std::cout << " " << tid;
        std::cout << " }" << std::endl;

        // ==== STEP 1: 搜集本轮可以立刻启动的 candidates ====
        std::vector<int> candidates;
        for (int tid : ready_set)
        {
          if (canceled[tid] || finished[tid]) 
            continue;
          const auto &t = tasks[tid];
          if (t.from_cluster >= 0 && t.to_cluster >= 0 &&
              t.from_cluster < static_cast<int>(send_next.size()) &&
              t.to_cluster < static_cast<int>(recv_next.size()) &&
              send_next[t.from_cluster] <= current_t &&
              recv_next[t.to_cluster] <= current_t)
          {
            candidates.push_back(tid);
          }
        }

        // ==== DEBUG: 打印可调度(candidates)任务 ====
        std::cout << "[debug] candidates:";
        for (int tid : candidates)
          std::cout << " " << tid;
        std::cout << std::endl;

        // ==== STEP 2: 如果有可调度的任务，选出优先启动的 ====
        if (!candidates.empty())
        {
          // pair_best: 每对(src,dst)只留分最高的一个
          std::map<std::pair<int, int>, int> pair_best;
          for (int tid : candidates)
          {
            auto key = std::make_pair(tasks[tid].from_cluster, tasks[tid].to_cluster);
            if (!pair_best.count(key) || score[tid] > score[pair_best[key]])
              pair_best[key] = tid;
          }

          std::vector<int> sorted;
          for (const auto &kv : pair_best)
            sorted.push_back(kv.second);
          std::sort(sorted.begin(), sorted.end(), [&](int a, int b) { return score[a] > score[b]; });

          // ==== DEBUG: 输出调度排序 ====
          std::cout << "[debug] sorted-tasks:";
          for (int tid : sorted)
            std::cout << " " << tid << "(" << score[tid] << ")";
          std::cout << std::endl;

          std::set<int> used_src, used_dst;
          std::vector<int> selected;
          for (int tid : sorted)
          {
            int src = tasks[tid].from_cluster;
            int dst = tasks[tid].to_cluster;
            int g = tasks[tid].alt_group;
            if (g >= 0 && g < static_cast<int>(alt_group_resolved.size()) && alt_group_resolved[g])
            {
              // alt_group 已决议后，只跳过互斥入口；保留已选路径的后续任务（alt_kind=2）。
              const int kind = tasks[tid].alt_kind;
              if (kind == 1 || kind == 3)
                continue;
            }
            if (used_src.count(src) || used_dst.count(dst))
              continue;
            used_src.insert(src);
            used_dst.insert(dst);
            selected.push_back(tid);
          }

          // ==== DEBUG: 输出被选中启动的任务 ====
          std::cout << "[debug] selected-tasks:";
          for (int tid : selected) std::cout << " " << tid;
          std::cout << std::endl;

          for (int tid : selected)
          {
            const auto &t = tasks[tid];
            int g = t.alt_group;
            if (g >= 0 && g < static_cast<int>(alt_group_resolved.size()) && !alt_group_resolved[g])
            {
              alt_group_resolved[g] = true;
              // 在线回退: 互斥任务取消
              for (int oid : alt_group_tasks[g])
              {
                if (oid == tid || canceled[oid] || finished[oid])
                  continue;
                const int okind = tasks[oid].alt_kind;
                const bool cancel_it =
                    (t.alt_kind == 3 && (okind == 1 || okind == 2)) ||
                    (t.alt_kind == 1 && okind == 3);
                if (cancel_it)
                {
                  canceled[oid] = true;
                  ready_set.erase(oid);
                  if (!finished[oid])
                  {
                    finished[oid] = true;
                    finished_cnt++;
                    std::cout << "[debug] cancel alt_task: " << oid << std::endl;
                  }
                }
              }
            }

            ready_set.erase(tid);
            double st = current_t;
            double ed = st + t.duration;
            send_next[t.from_cluster] = ed;
            recv_next[t.to_cluster] = ed;
            running.push({ed, tid});
            scheduled.push_back({tid, t.decision_id, t.from_cluster, t.to_cluster, t.payload, t.duration, st, ed, t.path_desc});
            std::cout << "[debug] start-task: " << tid << " at t=" << st << " ends t=" << ed
                      << " : " << t.path_desc << std::endl;
          }
          // 下一步推进到最早结束事件
        }

        // ==== 检查卡住? 没有更多可运行任务 ====
        if (running.empty())
        {
          if (finished_cnt >= n)
          {
            break;
          }
          // 无运行任务但未完成：推进到最近可用时刻
          double next_t = std::numeric_limits<double>::infinity();
          for (int tid : ready_set)
          {
            const auto &t = tasks[tid];
            if (t.from_cluster >= 0 && t.to_cluster >= 0 &&
                t.from_cluster < static_cast<int>(send_next.size()) &&
                t.to_cluster < static_cast<int>(recv_next.size()))
            {
              next_t = std::min(next_t, std::max(send_next[t.from_cluster], recv_next[t.to_cluster]));
            }
          }
          // ==== DEBUG: 卡住可能，打印下一跳时刻 ====
          if (next_t == std::numeric_limits<double>::infinity()) {
            std::cout << "[debug] 卡住！ready_set剩余任务也等不到资源可用，可能死锁！" << std::endl;
            break;
          }
          if (next_t <= current_t + 1e-12)
          {
            std::cout << "[debug] 卡住！next_t 未前进 (next_t=" << next_t
                      << ", current_t=" << current_t << ")，终止调度循环避免活锁。" << std::endl;
            break;
          }
          std::cout << "[debug] 无可运行任务, 推进到 next_t=" << next_t << std::endl;
          current_t = next_t;
          continue;
        }

        // ==== STEP 3: 事件推进，处理最早结束的一批任务 ====
        double next_finish = running.top().end_time;
        current_t = next_finish;
        std::vector<int> finished_now;
        while (!running.empty() && std::abs(running.top().end_time - next_finish) < 1e-12)
        {
          int tid = running.top().task_id;
          running.pop();
          if (!finished[tid])
          {
            finished[tid] = true;
            finished_cnt++;
            finished_now.push_back(tid);
            std::cout << "[debug] finish-task: " << tid << " at t=" << current_t << std::endl;
          }
        }

        for (int u : finished_now)
        {
          for (int v : succ[u])
          {
            remaining_pred[v]--;
            if (remaining_pred[v] == 0)
            {
              ready_set.insert(v);
              std::cout << "[debug] ready now: " << v << std::endl;
            }
          }
        }
      }

      // == 调度循环结束 ==
      std::cout << "[debug] 调度循环结束，共完成: " << finished_cnt << " / " << n << " 个任务" << std::endl;

      return scheduled;
    }

    bool has_intersection(const DataSliceUpdate &a, const DataSliceUpdate &b)
    {
      const int a_end = a.offset + a.size;
      const int b_end = b.offset + b.size;
      return !(a_end <= b.offset || b_end <= a.offset);
    }

    std::vector<std::vector<DataSliceUpdate>> split_by_intersection(std::vector<DataSliceUpdate> updates)
    {
      std::vector<std::vector<DataSliceUpdate>> groups;
      if (updates.empty())
      {
        return groups;
      }
      std::sort(updates.begin(), updates.end(), [](const DataSliceUpdate &a, const DataSliceUpdate &b) {
        if (a.offset != b.offset)
          return a.offset < b.offset;
        return a.block_id < b.block_id;
      });

      std::vector<DataSliceUpdate> current_group;
      current_group.push_back(updates[0]);
      int current_end = updates[0].offset + updates[0].size;
      for (size_t i = 1; i < updates.size(); i++)
      {
        if (updates[i].offset < current_end)
        {
          current_group.push_back(updates[i]);
          current_end = std::max(current_end, updates[i].offset + updates[i].size);
        }
        else
        {
          groups.push_back(current_group);
          current_group.clear();
          current_group.push_back(updates[i]);
          current_end = updates[i].offset + updates[i].size;
        }
      }
      groups.push_back(current_group);
      return groups;
    }

    double estimate_bandwidth_between_clusters(int from_cluster, int to_cluster)
    {
      // 带宽矩阵（区域顺序）：TYO, MEL, SG, SEO, JAK, HK
      // 仅录入上三角与对角线；下三角通过对称性查询。
      static const double bw_upper[6][6] = {
          {43.62, 4.21, 4.69, 5.98, 4.69, 5.37},
          {0.00, 51.33, 5.51, 3.23, 5.24, 4.96},
          {0.00, 0.00, 39.68, 5.52, 7.41, 5.53},
          {0.00, 0.00, 0.00, 32.54, 4.19, 5.19},
          {0.00, 0.00, 0.00, 0.00, 46.82, 4.47},
          {0.00, 0.00, 0.00, 0.00, 0.00, 35.87},
      };

      if (from_cluster < 0 || to_cluster < 0)
      {
        return 0.0;
      }
      if (from_cluster >= 6 || to_cluster >= 6)
      {
        // 当前矩阵只覆盖 6 个 cluster；超出范围时给一个保守默认值，避免崩溃。
        return (from_cluster == to_cluster) ? 35.0 : 1.0;
      }

      int i = std::min(from_cluster, to_cluster);
      int j = std::max(from_cluster, to_cluster);
      return bw_upper[i][j];
    }

    int get_local_parity_block_id(const Stripe *stripe, int group_id)
    {
      // 不能依赖固定区间编号（不同placement可能不一致），优先按 group 找本地校验块。
      for (const auto *block : stripe->blocks)
      {
        if (block->block_type == 'L' && block->map2group == group_id)
        {
          return block->block_id;
        }
      }
      // 回退：返回任意本地校验块（至少保证有可用cluster代表）。
      for (const auto *block : stripe->blocks)
      {
        if (block->block_type == 'L')
        {
          return block->block_id;
        }
      }
      return -1;
    }

    int get_global_parity_block_id(const Stripe *stripe, int group_id, const std::string &code_type)
    {
      if (stripe == nullptr)
      {
        throw std::runtime_error("stripe is null while finding global parity");
      }

      // XueLRC 采用 stripe 级 global parity：选择一个确定性的锚点（最小 block_id 的 G 块）。
      if (code_type == "XueLRC")
      {
        int best_gid = -1;
        for (const auto *block : stripe->blocks)
        {
          if (block->block_type == 'G')
          {
            if (best_gid < 0 || block->block_id < best_gid)
            {
              best_gid = block->block_id;
            }
          }
        }
        if (best_gid >= 0)
        {
          return best_gid;
        }
        throw std::runtime_error("Global parity block not found for XueLRC stripe-level rule");
      }

      // 其他编码（如 Uni/Azure）维持按 group 严格匹配。
      for (const auto *block : stripe->blocks)
      {
        if (block->block_type == 'G' && block->map2group == group_id)
        {
          return block->block_id;
        }
      }
      throw std::runtime_error("Global parity block not found for group_id=" + std::to_string(group_id));
    }

    int get_xue_global_parity_cluster_id(const Stripe *stripe)
    {
      for (const auto *block : stripe->blocks)
      {
        if (block->block_type == 'G')
        {
          return block->map2cluster;
        }
      }
      return -1;
    }

    bool xue_plan_group_has_data_update(
        const Stripe *stripe,
        int group_index,
        const std::map<int, std::vector<std::pair<int, int>>> &block_to_slices)
    {
      const int data_begin = group_index * stripe->k / stripe->z;
      const int data_end = (group_index + 1) * stripe->k / stripe->z;
      for (int j = data_begin; j < data_end; ++j)
      {
        if (block_to_slices.find(j) != block_to_slices.end())
        {
          return true;
        }
      }
      return false;
    }

    int get_group_id_for_data_block(const Stripe *stripe, int data_block_id)
    {
      for (const auto &entry : stripe->group_to_blocks)
      {
        const std::vector<int> &block_ids = entry.second;
        if (std::find(block_ids.begin(), block_ids.end(), data_block_id) != block_ids.end())
        {
          return entry.first;
        }
      }
      return -1;
    }

    const Block *find_block_by_id(const Stripe *stripe, int block_id)
    {
      for (const auto *block : stripe->blocks)
      {
        if (block->block_id == block_id)
        {
          return block;
        }
      }
      return nullptr;
    }

    int xue_class_subgroup_from_kind(DataUpdateClass klass)
    {
      switch (klass)
      {
      case DataUpdateClass::kLocalParitySameCluster:
        return 1;
      case DataUpdateClass::kGlobalParitySameCluster:
        return 2;
      case DataUpdateClass::kDataOnlyCluster:
        return 3;
      default:
        return 0;
      }
    }

    DataUpdateClass classify_xue_data_block(const Stripe *stripe, int data_block_id,
                                            int global_parity_cluster_id, const Block *lp_blk)
    {
      const Block *data_block = find_block_by_id(stripe, data_block_id);
      if (data_block == nullptr || lp_blk == nullptr)
      {
        return DataUpdateClass::kDataOnlyCluster;
      }
      if (data_block->map2cluster == lp_blk->map2cluster)
      {
        return DataUpdateClass::kLocalParitySameCluster;
      }
      if (global_parity_cluster_id >= 0 && data_block->map2cluster == global_parity_cluster_id)
      {
        return DataUpdateClass::kGlobalParitySameCluster;
      }
      return DataUpdateClass::kDataOnlyCluster;
    }

    XueUpdateResult xue_update(
        Stripe *stripe,
        const std::map<int, std::pair<int, int>> &block_to_slice_sizes,
        const std::string &code_type)
    {
      XueUpdateResult result;
      std::map<int, int> &group_to_ingress_cluster = result.group_to_ingress_cluster;
      if (stripe == nullptr)
      {
        return result;
      }

      std::vector<DataSliceUpdate> class2_updates;
      std::vector<DataSliceUpdate> class1_updates;
      std::map<std::pair<int, int>, std::vector<DataSliceUpdate>> class3_by_cluster_and_group;
      std::vector<TransferPlanDecision> decisions;

      // 1) 找到此次请求涉及的“数据块更新”
      for (const auto &entry : block_to_slice_sizes)
      {
        int block_id = entry.first;
        if (block_id < 0 || block_id >= stripe->k)
        {
          continue;
        }
        int group_id = get_group_id_for_data_block(stripe, block_id);
        if (group_id < 0)
        {
          continue;
        }
        int local_parity_id = get_local_parity_block_id(stripe, group_id);
        int global_parity_id = get_global_parity_block_id(stripe, group_id, code_type);
        if (local_parity_id < 0 || global_parity_id < 0 ||
            local_parity_id >= stripe->n || global_parity_id >= stripe->n)
        {
          continue;
        }
        const Block *data_block = find_block_by_id(stripe, block_id);
        const Block *local_block = find_block_by_id(stripe, local_parity_id);
        const Block *global_block = find_block_by_id(stripe, global_parity_id);
        if (data_block == nullptr || local_block == nullptr || global_block == nullptr)
        {
          continue;
        }

        DataSliceUpdate u;
        u.block_id = block_id;
        u.group_id = group_id;
        u.offset = entry.second.second;
        u.size = entry.second.first;
        u.data_cluster = data_block->map2cluster;
        u.local_parity_cluster = local_block->map2cluster;
        u.global_parity_cluster = global_block->map2cluster;

        if (u.data_cluster == u.local_parity_cluster)
        {
          u.klass = DataUpdateClass::kLocalParitySameCluster;
          class1_updates.push_back(u);
          continue;
        }
        if (u.data_cluster == u.global_parity_cluster)
        {
          u.klass = DataUpdateClass::kGlobalParitySameCluster;
          class2_updates.push_back(u);
        }
        else
        {
          u.klass = DataUpdateClass::kDataOnlyCluster;
          class3_by_cluster_and_group[std::make_pair(u.data_cluster, u.group_id)].push_back(u);
        }
      }

      // 1.5) 第1类：直传到全局校验cluster，或通过中转cluster两跳传输
      // 规则：若存在中转cluster relay，使得 bw(data,global) < bw(relay,global)，则选中转；否则直传。
      for (const auto &u : class1_updates)
      {
        const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
        const double bw_direct = estimate_bandwidth_between_clusters(u.data_cluster, u.global_parity_cluster);
        int best_relay = -1;
        double best_relay_to_global_bw = -1.0;

        // 候选中转cluster：遍历stripe内可见cluster（避免依赖固定cluster数量）
        std::set<int> candidate_clusters(stripe->place2clusters.begin(), stripe->place2clusters.end());
        candidate_clusters.insert(u.data_cluster);
        candidate_clusters.insert(u.global_parity_cluster);
        for (int relay : candidate_clusters)
        {
          if (relay == u.data_cluster || relay == u.global_parity_cluster)
            continue;
          const double bw_relay_to_global = estimate_bandwidth_between_clusters(relay, u.global_parity_cluster);
          if (bw_relay_to_global > best_relay_to_global_bw)
          {
            best_relay_to_global_bw = bw_relay_to_global;
            best_relay = relay;
          }
        }

        TransferPlanDecision d;
        d.hot_cluster = u.global_parity_cluster;
        d.block_ids = {u.block_id};
        if (best_relay >= 0 && bw_direct < best_relay_to_global_bw)
        {
          d.reason = "class1: choose relay to global parity cluster";
          d.steps.push_back({u.data_cluster, best_relay, "data_delta", false,
                             fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                 fmt_cluster_id(best_relay) + " (数据增量)", static_cast<double>(u.size)});
          d.steps.push_back({best_relay, u.global_parity_cluster, "data_delta", true,
                             fmt_cluster_id(best_relay) + " 转发数据增量 -> " +
                                 fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id),
                             static_cast<double>(u.size)});
          d.has_direct_fallback = true;
          d.direct_fallback_dst = u.global_parity_cluster;
          group_to_ingress_cluster[u.group_id] = u.data_cluster;
          Class1RelayRoute route;
          route.enabled = true;
          route.relay_cluster = best_relay;
          route.global_parity_cluster = u.global_parity_cluster;
          result.group_to_class1_relay[u.group_id] = route;
        }
        else
        {
          d.reason = "class1: direct transfer to global parity cluster";
          d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                             fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                 fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), static_cast<double>(u.size)});
          group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
        }
        decisions.push_back(d);
      }

      // 2) 第2类：同块多切片合并为单决策、单步批量传到本地校验块所在 cluster
      std::sort(class2_updates.begin(), class2_updates.end(), [](const DataSliceUpdate &a, const DataSliceUpdate &b) {
        if (a.block_id != b.block_id)
        {
          return a.block_id < b.block_id;
        }
        if (a.offset != b.offset)
        {
          return a.offset < b.offset;
        }
        return a.size < b.size;
      });
      for (size_t c2_i = 0; c2_i < class2_updates.size();)
      {
        size_t c2_j = c2_i + 1;
        while (c2_j < class2_updates.size() &&
               class2_updates[c2_j].block_id == class2_updates[c2_i].block_id)
        {
          ++c2_j;
        }
        std::vector<DataSliceUpdate> block_slices;
        block_slices.reserve(c2_j - c2_i);
        for (size_t t = c2_i; t < c2_j; ++t)
        {
          block_slices.push_back(class2_updates[t]);
        }
        const DataSliceUpdate &u = class2_updates[c2_i];
        const int lp_id = get_local_parity_block_id(stripe, u.group_id);
        group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
        TransferPlanDecision d;
        d.hot_cluster = u.global_parity_cluster;
        d.block_ids = {u.block_id};
        d.reason = "class2: data and global parity colocated, send update delta to local parity cluster";
        const double batch_sz = sum_slice_transfer_bytes(block_slices);
        const std::string batch_range = fmt_block_multi_slice_ranges(u.block_id, block_slices);
        d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                           fmt_cluster_id(u.data_cluster) + " 上 " + batch_range + " -> " +
                               fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id), batch_sz});
        decisions.push_back(d);
        c2_i = c2_j;
      }

      // 3) 第3类：仅在“同一 data cluster 且同一本地组”内按 offset 相交分组
      for (auto &kv : class3_by_cluster_and_group)
      {
        std::vector<DataSliceUpdate> updates = kv.second;
        auto grouped = split_by_intersection(updates);
        for (auto &g : grouped)
        {
          if (g.empty())
          {
            continue;
          }
          bool any_intersection = false;
          for (size_t i = 0; i < g.size() && !any_intersection; i++)
          {
            for (size_t j = i + 1; j < g.size(); j++)
            {
              if (has_intersection(g[i], g[j]))
              {
                any_intersection = true;
                break;
              }
            }
          }

          if (any_intersection)
          {
            // 第3类相交集合：
            //   (a) 发送各数据块增量到全局校验块cluster
            //   (b) 再在“到本地校验cluster”和“到全局校验cluster”中择高带宽目标发送校验增量
            TransferPlanDecision d;
            d.hot_cluster = g.front().global_parity_cluster;
            d.reason = "class3-overlap: send data delta to global parity cluster, then choose parity path by max(data->local, global->local)";
            for (const auto &u : g)
            {
              const int lp_id = get_local_parity_block_id(stripe, u.group_id);
              const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
              d.block_ids.push_back(u.block_id);
              d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                                 fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                     fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), static_cast<double>(u.size)});
              const double bw_data_to_local = estimate_bandwidth_between_clusters(u.data_cluster, u.local_parity_cluster);
              const double bw_global_to_local = estimate_bandwidth_between_clusters(u.global_parity_cluster, u.local_parity_cluster);
              if (bw_data_to_local >= bw_global_to_local)
              {
                // data->local 更优：由 data 所在 cluster 直接发送 parity_delta 到 local parity。
                group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
                d.steps.push_back({u.data_cluster, u.local_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.data_cluster) + " 上 基于 " + fmt_data_update_range(u) + " 的校验更新 -> " +
                                       fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id),
                                   static_cast<double>(u.size)});
              }
              else
              {
                // global->local 更优：由 global parity 所在 cluster 转发 parity_delta 到 local parity。
                group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
                d.steps.push_back({u.global_parity_cluster, u.local_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id) +
                                       " 侧校验衍生数据 -> " + fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id),
                                   static_cast<double>(u.size)});
              }
            }
            decisions.push_back(d);
          }
          else
          {
            // 第3类不相交集合：
            // 三种候选路径：
            // 1) data->global 与 data->local（并行双发）
            // 2) data->local -> global（串行）
            // 3) data->global -> local（串行）
            TransferPlanDecision d;
            d.hot_cluster = g.front().global_parity_cluster;
            d.reason = "class3-disjoint: choose best of three routes (parallel direct / local-first / global-first)";
            for (const auto &u : g)
            {
              const int lp_id = get_local_parity_block_id(stripe, u.group_id);
              const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
              d.block_ids.push_back(u.block_id);

              const double bw_d2g = estimate_bandwidth_between_clusters(u.data_cluster, u.global_parity_cluster);
              const double bw_d2l = estimate_bandwidth_between_clusters(u.data_cluster, u.local_parity_cluster);
              const double bw_g2l = estimate_bandwidth_between_clusters(u.global_parity_cluster, u.local_parity_cluster);
              const double bw_l2g = estimate_bandwidth_between_clusters(u.local_parity_cluster, u.global_parity_cluster);

              // 路径1：并行双发（data->global + data->local）
              const double score_parallel_direct = bw_d2g + bw_d2l;
              // 路径2：data->local -> global
              const double score_local_first = bw_d2l + bw_l2g;
              // 路径3：data->global -> local
              const double score_global_first = bw_d2g + bw_g2l;

              if (score_parallel_direct >= score_local_first && score_parallel_direct >= score_global_first)
              {
                // 路径1：并行双发到两个校验cluster
                d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                       fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id)});
                d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                       fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id)});
                // 这里给客户端入口选 data->global / data->local 中带宽更高的一侧
                group_to_ingress_cluster[u.group_id] = (bw_d2g >= bw_d2l) ? u.global_parity_cluster : u.local_parity_cluster;
              }
              else if (score_local_first >= score_global_first)
              {
                // 路径2：step1 data -> local, step2 local -> global（串行）
                d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                       fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id)});
                d.steps.push_back({u.local_parity_cluster, u.global_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id) +
                                       " 侧校验衍生数据 -> " + fmt_cluster_id(u.global_parity_cluster) + " " +
                                       fmt_global_parity_block(gp_id)});
                group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
              }
              else
              {
                // 路径3：step1 data -> global, step2 global -> local（串行）
                d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + fmt_data_update_range(u) + " -> " +
                                       fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id)});
                d.steps.push_back({u.global_parity_cluster, u.local_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id) +
                                       " 侧校验衍生数据 -> " + fmt_cluster_id(u.local_parity_cluster) + " " +
                                       fmt_local_parity_block(lp_id)});
                group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
              }
            }
            decisions.push_back(d);
          }
        }
      }

      log_append_route_decisions(decisions);

      // 根据依赖 + 机架端口约束做时间调度，输出“谁先谁后、谁可并发”
      std::vector<ScheduledTask> schedule = schedule_transfer_steps(decisions);
      log_append_schedule_visual(schedule);
      result.route_decisions = std::move(decisions);
      result.scheduled_tasks = std::move(schedule);
      return result;
    }

    XueUpdateResult xue_update_sparse(
        Stripe *stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_to_slices,
        const std::string &code_type)
    {
      XueUpdateResult result;
      std::map<int, int> &group_to_ingress_cluster = result.group_to_ingress_cluster;
      if (stripe == nullptr)
      {
        return result;
      }

      std::vector<DataSliceUpdate> class2_updates;
      std::vector<DataSliceUpdate> class1_updates;
      std::map<std::pair<int, int>, std::vector<DataSliceUpdate>> class3_by_cluster_and_group;
      std::vector<TransferPlanDecision> decisions;

      for (const auto &entry : block_to_slices)
      {
        int block_id = entry.first;
        if (block_id < 0 || block_id >= stripe->k)
        {
          continue;
        }
        int group_id = get_group_id_for_data_block(stripe, block_id);
        if (group_id < 0)
        {
          continue;
        }
        int local_parity_id = get_local_parity_block_id(stripe, group_id);
        int global_parity_id = get_global_parity_block_id(stripe, group_id, code_type);
        if (local_parity_id < 0 || global_parity_id < 0 ||
            local_parity_id >= stripe->n || global_parity_id >= stripe->n)
        {
          continue;
        }
        const Block *data_block = find_block_by_id(stripe, block_id);
        const Block *local_block = find_block_by_id(stripe, local_parity_id);
        const Block *global_block = find_block_by_id(stripe, global_parity_id);
        if (data_block == nullptr || local_block == nullptr || global_block == nullptr)
        {
          continue;
        }

        for (const auto &slice : entry.second)
        {
          DataSliceUpdate u;
          u.block_id = block_id;
          u.group_id = group_id;
          u.size = slice.first;
          u.offset = slice.second;
          u.data_cluster = data_block->map2cluster;
          u.local_parity_cluster = local_block->map2cluster;
          u.global_parity_cluster = global_block->map2cluster;
          if (u.data_cluster == u.local_parity_cluster)
          {
            u.klass = DataUpdateClass::kLocalParitySameCluster;
            class1_updates.push_back(u);
          }
          else if (u.data_cluster == u.global_parity_cluster)
          {
            u.klass = DataUpdateClass::kGlobalParitySameCluster;
            class2_updates.push_back(u);
          }
          else
          {
            u.klass = DataUpdateClass::kDataOnlyCluster;
            class3_by_cluster_and_group[std::make_pair(u.data_cluster, u.group_id)].push_back(u);
          }
        }
      }

      // 同一数据块多切片合并为单个决策与单步批量传输（sparse）
      std::sort(class1_updates.begin(), class1_updates.end(), [](const DataSliceUpdate &a, const DataSliceUpdate &b) {
        if (a.block_id != b.block_id)
        {
          return a.block_id < b.block_id;
        }
        if (a.offset != b.offset)
        {
          return a.offset < b.offset;
        }
        return a.size < b.size;
      });
      for (size_t class1_i = 0; class1_i < class1_updates.size();)
      {
        size_t class1_j = class1_i + 1;
        while (class1_j < class1_updates.size() &&
               class1_updates[class1_j].block_id == class1_updates[class1_i].block_id)
        {
          ++class1_j;
        }
        std::vector<DataSliceUpdate> block_slices;
        block_slices.reserve(class1_j - class1_i);
        for (size_t t = class1_i; t < class1_j; ++t)
        {
          block_slices.push_back(class1_updates[t]);
        }
        const DataSliceUpdate &u = class1_updates[class1_i];
        const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
        const double bw_direct = estimate_bandwidth_between_clusters(u.data_cluster, u.global_parity_cluster);
        int best_relay = -1;
        double best_relay_to_global_bw = -1.0;
        std::set<int> candidate_clusters(stripe->place2clusters.begin(), stripe->place2clusters.end());
        candidate_clusters.insert(u.data_cluster);
        candidate_clusters.insert(u.global_parity_cluster);
        for (int relay : candidate_clusters)
        {
          if (relay == u.data_cluster || relay == u.global_parity_cluster)
            continue;
          const double bw_relay_to_global = estimate_bandwidth_between_clusters(relay, u.global_parity_cluster);
          if (bw_relay_to_global > best_relay_to_global_bw)
          {
            best_relay_to_global_bw = bw_relay_to_global;
            best_relay = relay;
          }
        }
        TransferPlanDecision d;
        d.hot_cluster = u.global_parity_cluster;
        d.block_ids = {u.block_id};
        const double batch_sz = sum_slice_transfer_bytes(block_slices);
        const std::string batch_range = fmt_block_multi_slice_ranges(u.block_id, block_slices);
        if (best_relay >= 0 && bw_direct < best_relay_to_global_bw)
        {
          d.reason = "class1: choose relay to global parity cluster";
          d.steps.push_back({u.data_cluster, best_relay, "data_delta", false,
                             fmt_cluster_id(u.data_cluster) + " 上 " + batch_range + " -> " +
                                 fmt_cluster_id(best_relay) + " (数据增量)", batch_sz});
          d.steps.push_back({best_relay, u.global_parity_cluster, "data_delta", true,
                             fmt_cluster_id(best_relay) + " 转发数据增量 -> " +
                                 fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id),
                             batch_sz});
          d.has_direct_fallback = true;
          d.direct_fallback_dst = u.global_parity_cluster;
          group_to_ingress_cluster[u.group_id] = u.data_cluster;
          Class1RelayRoute route;
          route.enabled = true;
          route.relay_cluster = best_relay;
          route.global_parity_cluster = u.global_parity_cluster;
          result.group_to_class1_relay[u.group_id] = route;
        }
        else
        {
          d.reason = "class1: direct transfer to global parity cluster";
          d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                             fmt_cluster_id(u.data_cluster) + " 上 " + batch_range + " -> " +
                                 fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), batch_sz});
          group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
        }
        decisions.push_back(d);
        class1_i = class1_j;
      }
      // 第2类：同块多切片合并为单决策、单步批量传输（sparse）
      std::sort(class2_updates.begin(), class2_updates.end(), [](const DataSliceUpdate &a, const DataSliceUpdate &b) {
        if (a.block_id != b.block_id)
        {
          return a.block_id < b.block_id;
        }
        if (a.offset != b.offset)
        {
          return a.offset < b.offset;
        }
        return a.size < b.size;
      });
      for (size_t c2_i = 0; c2_i < class2_updates.size();)
      {
        size_t c2_j = c2_i + 1;
        while (c2_j < class2_updates.size() &&
               class2_updates[c2_j].block_id == class2_updates[c2_i].block_id)
        {
          ++c2_j;
        }
        std::vector<DataSliceUpdate> block_slices;
        block_slices.reserve(c2_j - c2_i);
        for (size_t t = c2_i; t < c2_j; ++t)
        {
          block_slices.push_back(class2_updates[t]);
        }
        const DataSliceUpdate &u = class2_updates[c2_i];
        const int lp_id = get_local_parity_block_id(stripe, u.group_id);
        group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
        TransferPlanDecision d;
        d.hot_cluster = u.global_parity_cluster;
        d.block_ids = {u.block_id};
        d.reason = "class2: data and global parity colocated, send update delta to local parity cluster";
        const double batch_sz = sum_slice_transfer_bytes(block_slices);
        const std::string batch_range = fmt_block_multi_slice_ranges(u.block_id, block_slices);
        d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                           fmt_cluster_id(u.data_cluster) + " 上 " + batch_range + " -> " +
                               fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id), batch_sz});
        decisions.push_back(d);
        c2_i = c2_j;
      }
      for (auto &kv : class3_by_cluster_and_group)
      {
        std::vector<DataSliceUpdate> updates = kv.second;
        auto grouped = split_by_intersection(updates);
        for (auto &g : grouped)
        {
          if (g.empty()) continue;
          bool any_intersection = false;
          for (size_t i = 0; i < g.size() && !any_intersection; i++)
          {
            for (size_t j = i + 1; j < g.size(); j++)
            {
              if (has_intersection(g[i], g[j])) { any_intersection = true; break; }
            }
          }
          TransferPlanDecision d;
          d.hot_cluster = g.front().global_parity_cluster;
          if (any_intersection)
          {
            d.reason = "class3-overlap: send data delta to global parity cluster, then choose parity path by max(data->local, global->local)";
            std::map<int, std::vector<DataSliceUpdate>> by_block_overlap;
            for (const auto &u : g)
            {
              by_block_overlap[u.block_id].push_back(u);
            }
            for (const auto &kv : by_block_overlap)
            {
              d.block_ids.push_back(kv.first);
            }
            for (const auto &kv : by_block_overlap)
            {
              const int bid = kv.first;
              const std::vector<DataSliceUpdate> &sls = kv.second;
              const DataSliceUpdate &u0 = sls.front();
              const int gp_id = get_global_parity_block_id(stripe, u0.group_id, code_type);
              const double batch_sz = sum_slice_transfer_bytes(sls);
              d.steps.push_back({u0.data_cluster, u0.global_parity_cluster, "data_delta", false,
                                 fmt_cluster_id(u0.data_cluster) + " 上 " + fmt_block_multi_slice_ranges(bid, sls) + " -> " +
                                     fmt_cluster_id(u0.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), batch_sz});
            }
            // 相交集合的 parity 传输采用“先并集后发送”：例如 [1,3] 与 [2,5] 合并为 [1,5]。
            bool same_group = true;
            const int base_group = g.front().group_id;
            const int base_local = g.front().local_parity_cluster;
            const int base_global = g.front().global_parity_cluster;
            for (const auto &u : g)
            {
              if (u.group_id != base_group ||
                  u.local_parity_cluster != base_local ||
                  u.global_parity_cluster != base_global)
              {
                same_group = false;
                break;
              }
            }
            if (same_group)
            {
              std::vector<std::pair<int, int>> merged_ranges; // [start, end] (闭区间，仅内部合并用)
              for (const auto &u : g)
              {
                merged_ranges.push_back({u.offset, u.offset + u.size - 1});
              }
              std::sort(merged_ranges.begin(), merged_ranges.end());
              std::vector<std::pair<int, int>> compact;
              for (const auto &rg : merged_ranges)
              {
                if (compact.empty() || rg.first > compact.back().second + 1)
                {
                  compact.push_back(rg);
                }
                else
                {
                  compact.back().second = std::max(compact.back().second, rg.second);
                }
              }

              const double bw_data_to_local = estimate_bandwidth_between_clusters(g.front().data_cluster, base_local);
              const double bw_global_to_local = estimate_bandwidth_between_clusters(base_global, base_local);
              const int lp_id = get_local_parity_block_id(stripe, base_group);
              const int gp_id = get_global_parity_block_id(stripe, base_group, code_type);
              for (const auto &rg : compact)
              {
                if (bw_data_to_local >= bw_global_to_local)
                {
                  // data->local 更优：由 data 所在 cluster 直接发送 parity_delta 到 local parity。
                  group_to_ingress_cluster[base_group] = base_local;
                  d.steps.push_back({g.front().data_cluster, base_local, "parity_delta", true,
                                     fmt_cluster_id(g.front().data_cluster) + " 上 基于 相交集合合并区间[" +
                                         std::to_string(rg.first) + "," + std::to_string(rg.second + 1) + ") 的校验更新 -> " +
                                         fmt_cluster_id(base_local) + " " + fmt_local_parity_block(lp_id),
                                     static_cast<double>(rg.second - rg.first + 1)});
                }
                else
                {
                  // global->local 更优：由 global parity 所在 cluster 转发 parity_delta 到 local parity。
                  group_to_ingress_cluster[base_group] = base_global;
                  d.steps.push_back({base_global, base_local, "parity_delta", true,
                                     fmt_cluster_id(base_global) + " " + fmt_global_parity_block(gp_id) +
                                         " 侧基于 相交集合合并区间[" + std::to_string(rg.first) + "," + std::to_string(rg.second + 1) +
                                         ") 的校验衍生数据 -> " + fmt_cluster_id(base_local) + " " + fmt_local_parity_block(lp_id),
                                     static_cast<double>(rg.second - rg.first + 1)});
                }
              }
            }
            else
            {
              // 兜底：跨组/跨 parity 时按数据块合并多切片为单步 parity_delta。
              for (const auto &kv : by_block_overlap)
              {
                const int bid = kv.first;
                const std::vector<DataSliceUpdate> &sls = kv.second;
                const DataSliceUpdate &u = sls.front();
                const int lp_id = get_local_parity_block_id(stripe, u.group_id);
                const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
                const double bw_data_to_local = estimate_bandwidth_between_clusters(u.data_cluster, u.local_parity_cluster);
                const double bw_global_to_local = estimate_bandwidth_between_clusters(u.global_parity_cluster, u.local_parity_cluster);
                const double batch_sz = sum_slice_transfer_bytes(sls);
                const std::string batch_desc = fmt_block_multi_slice_ranges(bid, sls);
                if (bw_data_to_local >= bw_global_to_local)
                {
                  group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
                  d.steps.push_back({u.data_cluster, u.local_parity_cluster, "parity_delta", true,
                                     fmt_cluster_id(u.data_cluster) + " 上 基于 " + batch_desc + " 的校验更新 -> " +
                                         fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id), batch_sz});
                }
                else
                {
                  group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
                  d.steps.push_back({u.global_parity_cluster, u.local_parity_cluster, "parity_delta", true,
                                     fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id) +
                                         " 侧基于 " + batch_desc + " 的校验衍生数据 -> " + fmt_cluster_id(u.local_parity_cluster) + " " +
                                         fmt_local_parity_block(lp_id), batch_sz});
                }
              }
            }
          }
          else
          {
            d.reason = "class3-disjoint: choose best of three routes (parallel direct / local-first / global-first)";
            std::map<int, std::vector<DataSliceUpdate>> by_block_disjoint;
            for (const auto &u : g)
            {
              by_block_disjoint[u.block_id].push_back(u);
            }
            for (auto &kv : by_block_disjoint)
            {
              std::sort(kv.second.begin(), kv.second.end(), [](const DataSliceUpdate &a, const DataSliceUpdate &b) {
                if (a.offset != b.offset)
                {
                  return a.offset < b.offset;
                }
                return a.size < b.size;
              });
            }
            for (const auto &kv : by_block_disjoint)
            {
              const int bid = kv.first;
              const std::vector<DataSliceUpdate> &sls = kv.second;
              const DataSliceUpdate &u = sls.front();
              d.block_ids.push_back(bid);
              const int lp_id = get_local_parity_block_id(stripe, u.group_id);
              const int gp_id = get_global_parity_block_id(stripe, u.group_id, code_type);
              const double batch_sz = sum_slice_transfer_bytes(sls);
              const std::string batch_desc = fmt_block_multi_slice_ranges(bid, sls);
              const double bw_d2g = estimate_bandwidth_between_clusters(u.data_cluster, u.global_parity_cluster);
              const double bw_d2l = estimate_bandwidth_between_clusters(u.data_cluster, u.local_parity_cluster);
              const double bw_g2l = estimate_bandwidth_between_clusters(u.global_parity_cluster, u.local_parity_cluster);
              const double bw_l2g = estimate_bandwidth_between_clusters(u.local_parity_cluster, u.global_parity_cluster);
              const double score_parallel_direct = bw_d2g + bw_d2l;
              const double score_local_first = bw_d2l + bw_l2g;
              const double score_global_first = bw_d2g + bw_g2l;
              if (score_parallel_direct >= score_local_first && score_parallel_direct >= score_global_first)
              {
                d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + batch_desc + " -> " +
                                       fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), batch_sz});
                d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + batch_desc + " -> " +
                                       fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id), batch_sz});
                group_to_ingress_cluster[u.group_id] = (bw_d2g >= bw_d2l) ? u.global_parity_cluster : u.local_parity_cluster;
              }
              else if (score_local_first >= score_global_first)
              {
                d.steps.push_back({u.data_cluster, u.local_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + batch_desc + " -> " +
                                       fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id), batch_sz});
                d.steps.push_back({u.local_parity_cluster, u.global_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.local_parity_cluster) + " " + fmt_local_parity_block(lp_id) +
                                       " 侧校验衍生数据 -> " + fmt_cluster_id(u.global_parity_cluster) + " " +
                                       fmt_global_parity_block(gp_id), batch_sz});
                group_to_ingress_cluster[u.group_id] = u.local_parity_cluster;
              }
              else
              {
                d.steps.push_back({u.data_cluster, u.global_parity_cluster, "data_delta", false,
                                   fmt_cluster_id(u.data_cluster) + " 上 " + batch_desc + " -> " +
                                       fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id), batch_sz});
                d.steps.push_back({u.global_parity_cluster, u.local_parity_cluster, "parity_delta", true,
                                   fmt_cluster_id(u.global_parity_cluster) + " " + fmt_global_parity_block(gp_id) +
                                       " 侧校验衍生数据 -> " + fmt_cluster_id(u.local_parity_cluster) + " " +
                                       fmt_local_parity_block(lp_id), batch_sz});
                group_to_ingress_cluster[u.group_id] = u.global_parity_cluster;
              }
            }
          }
          decisions.push_back(d);
        }
      }

      log_append_route_decisions(decisions);
      // debug
      std::cout << "[debug] START schedule_transfer_steps" << std::endl;
      std::vector<ScheduledTask> schedule = schedule_transfer_steps(decisions);
      std::cout << "[debug] END schedule_transfer_steps" << std::endl;
      log_append_schedule_visual(schedule);
      result.route_decisions = std::move(decisions);
      result.scheduled_tasks = std::move(schedule);
      return result;
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
    proxyIPPort->set_proxyport(selected_proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
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
    // Xue placement definition:
    // k: data blocks, r: local parity blocks (and local groups), z: global parity blocks.
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);
    assert(stripe->r > 0 && stripe->z > 0);
    assert(stripe->k % stripe->r == 0 && "Xue placement requires k % r == 0");

    const int k = stripe->k;
    const int r = stripe->r; // local group number == local parity number
    const int z = stripe->z; // global parity number
    const int h = k / r;
    const int cluster_num = m_sys_config->ClusterNum;
    const int global_cluster_id = stripe->stripe_id % cluster_num;
    int cluster_cursor = (global_cluster_id + 1) % cluster_num;

    auto next_cluster = [&](bool avoid_global) -> int
    {
      int cid = cluster_cursor % cluster_num;
      cluster_cursor++;
      if (avoid_global && cluster_num > 1 && cid == global_cluster_id)
      {
        cid = cluster_cursor % cluster_num;
        cluster_cursor++;
      }
      return cid;
    };

    auto place_block = [&](int block_idx, int cluster_id)
    {
      int t_node_id = randomly_select_a_node(cluster_id, stripe->stripe_id);
      blocks_info[block_idx].map2cluster = cluster_id;
      blocks_info[block_idx].map2node = t_node_id;
      update_stripe_info_in_node(t_node_id, stripe->stripe_id, block_idx);
      m_cluster_table[cluster_id].blocks.push_back(&blocks_info[block_idx]);
      m_cluster_table[cluster_id].stripes.insert(stripe->stripe_id);
      stripe->blocks.push_back(&blocks_info[block_idx]);
      stripe->place2clusters.insert(cluster_id);
      add_to_map(stripe->group_to_blocks, blocks_info[block_idx].map2group, block_idx);
    };

    // Build block metadata with Xue numbering:
    // [0, k): data, [k, k + r): local parity, [k + r, k + r + z): global parity.
    for (int i = 0; i < stripe->n; i++)
    {
      blocks_info[i].block_size = m_sys_config->BlockSize;
      blocks_info[i].map2stripe = stripe->stripe_id;
      blocks_info[i].map2key = stripe->object_keys[0];
      if (i < k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = i / h;
      }
      else if (i < k + r)
      {
        const int local_idx = i - k;
        std::string tmp = "_L";
        if (local_idx < 10)
          tmp = "_L0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(local_idx);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = local_idx;
      }
      else
      {
        const int global_idx = i - k - r;
        std::string tmp = "_G";
        if (global_idx < 10)
          tmp = "_G0";
        blocks_info[i].block_key = std::to_string(stripe->stripe_id) + tmp + std::to_string(global_idx);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = r;
      }
    }

    // Step 1: place all global parity blocks in one rotating cluster.
    for (int i = 0; i < z; i++)
    {
      place_block(k + r + i, global_cluster_id);
    }

    // Track remaining data range per local group after Steps 2-4.
    std::vector<int> remain_start(r, 0);
    std::vector<int> remain_count(r, 0);

    // Step 2-4 per local group.
    for (int g = 0; g < r; g++)
    {
      int group_data_begin = g * h;
      int consumed = 0;

      // Step 2: place z data + local parity in one cluster for this local group.
      int primary_cluster_id = next_cluster(true);
      int first_data_num = std::min(z, h);
      for (int t = 0; t < first_data_num; t++)
      {
        place_block(group_data_begin + t, primary_cluster_id);
      }
      place_block(k + g, primary_cluster_id);
      consumed += first_data_num;

      if (consumed >= h)
      {
        continue;
      }

      // Step 3: place one data block with global parity cluster.
      place_block(group_data_begin + consumed, global_cluster_id);
      consumed++;

      // Step 4: place each z+1 data blocks into one cluster.
      while (consumed + (z + 1) <= h)
      {
        int chunk_cluster_id = next_cluster(true);
        for (int t = 0; t < z + 1; t++)
        {
          place_block(group_data_begin + consumed + t, chunk_cluster_id);
        }
        consumed += z + 1;
      }

      remain_start[g] = group_data_begin + consumed;
      remain_count[g] = h - consumed;
    }

    // Step 5: m = (h - z - 1) mod (z + 1), aggregate leftovers from theta groups.
    int m = 0;
    for (int g = 0; g < r; g++)
    {
      if (remain_count[g] > 0)
      {
        m = remain_count[g];
        break;
      }
    }
    if (m > 0)
    {
      int theta = 1;
      if (m > 1)
      {
        theta = std::max(1, z / (m - 1));
      }
      else
      {
        theta = r;
      }
      for (int g = 0; g < r;)
      {
        int batch_cluster_id = next_cluster(true);
        int grouped = 0;
        while (g < r && grouped < theta)
        {
          for (int t = 0; t < remain_count[g]; t++)
          {
            place_block(remain_start[g] + t, batch_cluster_id);
          }
          g++;
          grouped++;
        }
      }
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
    plan.add_block_cluster_ids(block->map2cluster);
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

    // xue_update: 统一封装“传输路径选择 + 时间调度”
    XueUpdateResult update_result = xue_update(stripe, block_to_slice_sizes, m_sys_config->CodeType);
    const std::map<int, int> &group_to_ingress_cluster = update_result.group_to_ingress_cluster;

    for (int i = 0; i < stripe->z; i++)
    {
      proxy_proto::AppendStripeDataPlacement plan;
      plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(getClusterAppendSize(stripe, block_to_slice_sizes, i, parity_slice_size));
      plan.set_is_merge_parity(is_merge_parity);
      auto ingress_it = group_to_ingress_cluster.find(i);
      if (ingress_it != group_to_ingress_cluster.end())
      {
        plan.set_cluster_id(ingress_it->second);
      }
      else
      {
        plan.set_cluster_id(stripe->blocks[stripe->group_to_blocks[i][0]]->map2cluster);
      }
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

      const auto cluster_plans = split_placement_by_map2cluster(plan, i);
      for (const auto &cp : cluster_plans)
      {
        append_plans.push_back(cp);
      }
    }

    if (append_mode == "UNILRC_MODE" || append_mode == "CACHED_MODE")
    {
      return merge_append_plans_by_cluster(std::move(append_plans));
    }
    return append_plans;
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
      else if (code_type == "XueLRC")
      {
        initialize_xue_tripe_placement(&t_stripe);
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

    // 3. notify proxies to receive data
    // need multiple proxies to receive data, so need multiple threads
    std::vector<std::thread> threads;
    for (const auto &plan : append_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    fill_reply_from_append_plans(this, append_plans, proxyIPPort);

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
    const std::string &client_id = request->client_id();
    const int stripe_id = request->stripe_id();

    auto stripe_it = m_stripe_table.find(stripe_id);
    if (stripe_it == m_stripe_table.end())
    {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "stripe_id not found");
    }
    Stripe *stripe = &stripe_it->second;
    if (request->ranges_size() <= 0)
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty ranges");
    }
    // 将多个不连续区间视为“同一时刻的一次联合更新”，并保留块内离散切片（稀疏更新）。
    std::map<int, std::vector<std::pair<int, int>>> block_to_slices;
    const int unit_size = static_cast<int>(m_sys_config->UnitSize);
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    auto add_sparse_slice = [](std::map<int, std::vector<std::pair<int, int>>> &dst,
                               int block_id, int len, int off) {
      if (len <= 0) return;
      auto &vec = dst[block_id];
      vec.push_back(std::make_pair(len, off));
      std::sort(vec.begin(), vec.end(), [](const auto &a, const auto &b) { return a.second < b.second; });
      std::vector<std::pair<int, int>> merged;
      for (const auto &s : vec)
      {
        const int cur_l = s.second;
        const int cur_r = s.second + s.first - 1;
        if (merged.empty())
        {
          merged.push_back(s);
          continue;
        }
        int prev_l = merged.back().second;
        int prev_r = merged.back().second + merged.back().first - 1;
        if (cur_l <= prev_r + 1)
        {
          const int new_r = std::max(prev_r, cur_r);
          merged.back().second = prev_l;
          merged.back().first = new_r - prev_l + 1;
        }
        else
        {
          merged.push_back(s);
        }
      }
      vec.swap(merged);
    };
    for (int rid = 0; rid < request->ranges_size(); rid++)
    {
      const auto &rg = request->ranges(rid);
      const int logical_offset_start = rg.logical_offset_start();
      const int logical_offset_end = rg.logical_offset_end();
      if (logical_offset_end <= logical_offset_start)
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "range must satisfy start < end for [start,end)");
      }

      int pos = logical_offset_start;
      while (pos < logical_offset_end)
      {
        const int block_id = pos / block_size;
        const int block_offset = pos % block_size;
        const int take = std::min(block_size - block_offset, logical_offset_end - pos);
        add_sparse_slice(block_to_slices, block_id, take, block_offset);
        pos += take;
      }
    }

    if (block_to_slices.empty())
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "no effective ranges");
    }

    // parity 按受影响 unit 生成离散切片，避免扩成整块。
    for (const auto &kv : block_to_slices)
    {
      const int block_id = kv.first;
      if (block_id < 0 || block_id >= stripe->k)
      {
        continue;
      }
      for (const auto &slice : kv.second)
      {
        const int block_off = slice.second;
        const int block_end = block_off + slice.first - 1;
        const int u0 = block_off / unit_size;
        const int u1 = block_end / unit_size;
        const int parity_off = u0 * unit_size;
        const int parity_end = std::min(block_size - 1, (u1 + 1) * unit_size - 1);
        const int parity_len = parity_end - parity_off + 1;
        for (int i = stripe->k; i < stripe->n; i++)
        {
          add_sparse_slice(block_to_slices, i, parity_len, parity_off);
        }
      }
    }
    const bool is_merge_parity = false;

    std::cout << "[XUE_UPDATE_SCOPE] stripe=" << stripe_id
              << " | ranges=" << request->ranges_size() << std::endl;
    std::cout << "  涉及数据块及区间:";
    for (const auto &kv : block_to_slices)
    {
      if (kv.first >= 0 && kv.first < stripe->k)
      {
        for (const auto &slice : kv.second)
        {
          const int block_off = slice.second;
          const int block_end = block_off + slice.first - 1;
          const int u0 = block_off / unit_size;
          const int u1 = block_end / unit_size;
          std::cout << " block" << kv.first
                    << "[+" << block_off << ".." << block_end
                    << " len " << slice.first
                    << " | unit " << u0 << ".." << u1 << "]";
        }
      }
    }
    std::cout << std::endl;
    //debug
    std::cout << "start xue_update_sparse" << std::endl;
    XueUpdateResult update_result = xue_update_sparse(stripe, block_to_slices, m_sys_config->CodeType);
    const std::map<int, int> &group_to_ingress_cluster = update_result.group_to_ingress_cluster;
    const std::map<int, Class1RelayRoute> &group_to_class1_relay = update_result.group_to_class1_relay;
    const std::vector<ScheduledTask> &scheduled_tasks = update_result.scheduled_tasks;
    std::cout << "end xue_update_sparse" << std::endl;
    // 在 uploadXueUpdate 入口处显式输出传输时间窗，避免依赖下层函数打印行为。
    log_append_schedule_visual(scheduled_tasks);
    std::cout << "end log_append_schedule_visual" << std::endl;
    std::vector<proxy_proto::AppendStripeDataPlacement> append_plans;
    const int global_parity_cluster_id = get_xue_global_parity_cluster_id(stripe);
    bool global_data_forward_assigned = false;
    auto add_all_global_parity_metadata = [&](proxy_proto::AppendStripeDataPlacement &plan,
                                              const auto &add_block_slices_fn) {
      for (int j = stripe->k + stripe->r; j < stripe->k + stripe->r + stripe->z; j++)
      {
        add_block_slices_fn(j, false);
      }
    };
    for (int i = 0; i < stripe->z; i++)
    {
      if (!xue_plan_group_has_data_update(stripe, i, block_to_slices))
      {
        continue;
      }
      const int lp_id = get_local_parity_block_id(stripe, i);
      const Block *lp_blk = find_block_by_id(stripe, lp_id);
      const int group_data_begin = i * stripe->k / stripe->z;
      const int group_data_end = (i + 1) * stripe->k / stripe->z;
      auto ingress_it = group_to_ingress_cluster.find(i);
      const auto relay_it = group_to_class1_relay.find(i);
      const bool class1_relay = relay_it != group_to_class1_relay.end() && relay_it->second.enabled;

      std::map<int, std::vector<int>> tcp_blocks_by_class;
      for (int j = group_data_begin; j < group_data_end; ++j)
      {
        if (block_to_slices.find(j) == block_to_slices.end())
        {
          continue;
        }
        const DataUpdateClass klass =
            classify_xue_data_block(stripe, j, global_parity_cluster_id, lp_blk);
        const int class_sub = xue_class_subgroup_from_kind(klass);
        tcp_blocks_by_class[class_sub].push_back(j);
      }

      auto push_cluster_plans = [&](proxy_proto::AppendStripeDataPlacement &plan) {
        if (plan.append_size() <= 0)
        {
          return;
        }
        const auto cluster_plans = split_placement_by_map2cluster(plan, i);
        for (const auto &cp : cluster_plans)
        {
          append_plans.push_back(cp);
        }
      };

      auto build_xue_subgroup_plan = [&](int class_sub,
                                         const std::vector<int> &tcp_data_blocks) {
        proxy_proto::AppendStripeDataPlacement plan;
        plan.set_key(m_toolbox->gen_append_key_xue_subgroup(stripe->stripe_id, i, class_sub));
        plan.set_stripe_id(stripe->stripe_id);
        plan.set_is_merge_parity(is_merge_parity);
        plan.set_append_mode("XUE_UPDATE");
        plan.set_is_serialized(true);
        plan.set_xue_class1_relay_path(false);
        plan.set_xue_data_slices_are_delta(false);
        plan.set_xue_compute_global_parity(false);
        if (ingress_it != group_to_ingress_cluster.end())
        {
          plan.set_cluster_id(ingress_it->second);
        }
        else if (!tcp_data_blocks.empty())
        {
          const Block *ingress_block = find_block_by_id(stripe, tcp_data_blocks.front());
          if (ingress_block != nullptr)
          {
            plan.set_cluster_id(ingress_block->map2cluster);
          }
        }
        if (global_parity_cluster_id >= 0)
        {
          plan.set_xue_global_parity_cluster_id(global_parity_cluster_id);
          if (!global_data_forward_assigned)
          {
            global_data_forward_assigned = true;
          }
        }
        if (class_sub == 2 && global_parity_cluster_id >= 0)
        {
          for (int bid : tcp_data_blocks)
          {
            const Block *db = find_block_by_id(stripe, bid);
            if (db != nullptr && db->map2cluster == global_parity_cluster_id)
            {
              plan.set_xue_compute_global_parity(true);
              break;
            }
          }
        }
        int plan_append_size = 0;
        int tcp_slice_count = 0;
        auto add_block_slices = [&](int block_id, bool count_tcp) {
          const Block *block = find_block_by_id(stripe, block_id);
          if (block == nullptr)
          {
            return;
          }
          auto it = block_to_slices.find(block_id);
          if (it == block_to_slices.end())
          {
            return;
          }
          for (const auto &slice : it->second)
          {
            addBlockToAppendPlan(plan, block, m_node_table[block->map2node], slice);
            if (count_tcp)
            {
              plan_append_size += slice.first;
              ++tcp_slice_count;
            }
          }
        };
        for (int bid : tcp_data_blocks)
        {
          add_block_slices(bid, true);
        }
        auto add_local_parity_meta = [&]() {
          for (int j = stripe->k + i * stripe->r / stripe->z;
               j < stripe->k + (i + 1) * stripe->r / stripe->z; j++)
          {
            add_block_slices(j, false);
          }
        };
        if (class_sub == 1 || class_sub == 2 || class_sub == 3)
        {
          add_local_parity_meta();
        }
        if (class_sub == 1 || class_sub == 2)
        {
          add_all_global_parity_metadata(plan, add_block_slices);
        }
        else if (class_sub == 3)
        {
          add_all_global_parity_metadata(plan, add_block_slices);
        }
        plan.set_xue_tcp_slice_count(tcp_slice_count);
        plan.set_append_size(plan_append_size);
        return plan;
      };

      if (class1_relay)
      {
        std::vector<int> all_tcp;
        for (int j = group_data_begin; j < group_data_end; ++j)
        {
          if (block_to_slices.find(j) != block_to_slices.end())
          {
            all_tcp.push_back(j);
          }
        }
        proxy_proto::AppendStripeDataPlacement plan =
            build_xue_subgroup_plan(1, all_tcp);
        plan.set_key(m_toolbox->gen_append_key(stripe->stripe_id, i));
        plan.set_xue_class1_relay_path(true);
        plan.set_xue_relay_cluster_id(relay_it->second.relay_cluster);
        plan.set_xue_global_parity_cluster_id(relay_it->second.global_parity_cluster);
        plan.set_xue_compute_global_parity(false);
        global_data_forward_assigned = true;
        push_cluster_plans(plan);
      }
      else
      {
        for (int class_sub = 1; class_sub <= 3; ++class_sub)
        {
          const auto it = tcp_blocks_by_class.find(class_sub);
          if (it == tcp_blocks_by_class.end() || it->second.empty())
          {
            continue;
          }
          proxy_proto::AppendStripeDataPlacement plan =
              build_xue_subgroup_plan(class_sub, it->second);
          push_cluster_plans(plan);
        }
      }
    }

    for (const auto &plan : append_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    for (const auto &plan : append_plans)
    {
      notify_proxies_ready(plan);
    }
    fill_reply_from_append_plans(this, append_plans, proxyIPPort);

    std::cout << "[XUE_UPDATE] client=" << client_id << " stripe=" << stripe_id
              << " merged_ranges=" << request->ranges_size() << std::endl;
    return grpc::Status::OK;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_add_plans(Stripe *stripe)
  {
    std::map<int, std::vector<int>> blocks_by_cluster;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      for (int bid : stripe->group_to_blocks[i])
      {
        const Block *block = find_block_by_id(stripe, bid);
        if (block == nullptr)
        {
          continue;
        }
        blocks_by_cluster[block->map2cluster].push_back(bid);
      }
    }
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (auto &kv : blocks_by_cluster)
    {
      std::sort(kv.second.begin(), kv.second.end());
      kv.second.erase(std::unique(kv.second.begin(), kv.second.end()), kv.second.end());
      proxy_proto::AppendStripeDataPlacement plan;
      const int cluster_id = kv.first;
      const int block_count = static_cast<int>(kv.second.size());
      size_t append_size = kv.second.size() * static_cast<size_t>(m_sys_config->BlockSize);
      plan.set_key(m_toolbox->gen_append_key_cluster_blocks(stripe->stripe_id, -1, cluster_id, kv.second));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_append_size(append_size);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);
      plan.set_xue_tcp_slice_count(block_count);
      for (int bid : kv.second)
      {
        const Block *block = find_block_by_id(stripe, bid);
        if (block == nullptr)
        {
          continue;
        }
        addBlockToAppendPlan(plan, block, m_node_table[block->map2node],
                             std::make_pair(m_sys_config->BlockSize, 0));
      }
      add_plans.push_back(plan);
    }

    return add_plans;
  }

  std::vector<proxy_proto::AppendStripeDataPlacement> CoordinatorImpl::generate_sub_add_plans(Stripe *stripe, size_t subset_size)
  {
    int data_block_num = subset_size / m_sys_config->BlockSize;
    int k = m_sys_config->k;
    std::map<int, std::vector<int>> blocks_by_cluster;
    for (int i = 0; i < stripe->num_groups; i++)
    {
      for (int bid : stripe->group_to_blocks[i])
      {
        if (bid < k && bid >= data_block_num)
        {
          continue;
        }
        const Block *block = find_block_by_id(stripe, bid);
        if (block == nullptr)
        {
          continue;
        }
        blocks_by_cluster[block->map2cluster].push_back(bid);
      }
    }
    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans;
    for (auto &kv : blocks_by_cluster)
    {
      std::sort(kv.second.begin(), kv.second.end());
      kv.second.erase(std::unique(kv.second.begin(), kv.second.end()), kv.second.end());
      proxy_proto::AppendStripeDataPlacement plan;
      const int cluster_id = kv.first;
      size_t append_size = kv.second.size() * static_cast<size_t>(m_sys_config->BlockSize);
      if (append_size == 0)
      {
        continue;
      }
      const int block_count = static_cast<int>(kv.second.size());
      plan.set_key(m_toolbox->gen_append_key_cluster_blocks(stripe->stripe_id, -1, cluster_id, kv.second));
      plan.set_stripe_id(stripe->stripe_id);
      plan.set_is_merge_parity(false);
      plan.set_cluster_id(cluster_id);
      plan.set_append_mode("UNILRC_MODE");
      plan.set_is_serialized(false);
      plan.set_append_size(append_size);
      plan.set_xue_tcp_slice_count(block_count);
      for (int bid : kv.second)
      {
        const Block *block = find_block_by_id(stripe, bid);
        if (block == nullptr)
        {
          continue;
        }
        addBlockToAppendPlan(plan, block, m_node_table[block->map2node],
                             std::make_pair(m_sys_config->BlockSize, 0));
      }
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
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "XueLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or XueLRC!");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
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
    else if (code_type == "XueLRC")
    {
      initialize_xue_tripe_placement(&t_stripe);
    }

    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_add_plans(&t_stripe);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    std::vector<std::thread> threads;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    fill_reply_from_append_plans(this, add_plans, proxyIPPort);

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
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "XueLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or XueLRC!");

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.n = m_sys_config->n;
    t_stripe.k = m_sys_config->k;
    t_stripe.r = m_sys_config->r;
    t_stripe.z = m_sys_config->z;
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
    else if (code_type == "XueLRC")
    {
      initialize_xue_tripe_placement(&t_stripe);
    }

    print_stripe_data_placement(t_stripe);

    std::vector<proxy_proto::AppendStripeDataPlacement> add_plans = generate_sub_add_plans(&t_stripe, setSizeBytes);

    for (const auto &plan : add_plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    std::vector<std::thread> threads;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
    }
    for (auto &thread : threads)
    {
      thread.join();
    }
    fill_reply_from_append_plans(this, add_plans, proxyIPPort);

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
        for (int i = 1; i <= z; i++)
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
      get_threads.push_back(std::thread([this, &stripe_block_ids, &client_ip, &client_port, &proxyIPPort, &unique_cluster_ids, i](){
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

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::RecoveryReply recovery_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
      recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
      recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
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
      else
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
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
          &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time
        ](){
          grpc::ClientContext degraded_read_context;
          proxy_proto::DegradedReadRequest degraded_read_request;
          proxy_proto::DegradedReadReply degraded_read_reply;
          degraded_read_request.set_clientip(dest_proxy_ip);
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
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
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id,
        &disk_io_start_time, &disk_io_end_time, &decode_start_time, &decode_end_time, &network_start_time, &network_end_time, 
        &grpc_notify_time, &grpc_start_time, &data_node_grpc_notify_time, &data_node_grpc_start_time, &cross_rack_network_time, &cross_rack_xor_time,
        &dest_data_node_network_time, &dest_data_node_disk_io_time
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
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
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

    if (recovery_group_ids.size() == 1)
    {
      //assert((code_type == "UniLRC") || (code_type == "AzureLRC" && (failed_block_id < m_sys_config->k || failed_block_id >= m_sys_config->k + m_sys_config->r)));

      grpc::ClientContext recovery_context;
      proxy_proto::RecoveryRequest recovery_request;
      proxy_proto::RecoveryReply recovery_reply;

      int chosen_cluster_id = get_cluster_id_by_group_id(t_stripe, recovery_group_ids[0]);
      std::string chosen_proxy = m_cluster_table[chosen_cluster_id].proxy_ip + ":" + std::to_string(m_cluster_table[chosen_cluster_id].proxy_port);
      recovery_request.set_failed_block_id(failed_block_id);
      recovery_request.set_failed_block_key(t_stripe.blocks[failed_block_id]->block_key);
      int t_node_id = randomly_select_a_node(chosen_cluster_id, stripe_id);
      recovery_request.set_replaced_node_ip(m_node_table[t_node_id].node_ip);
      recovery_request.set_replaced_node_port(m_node_table[t_node_id].node_port);
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

      status = m_proxy_ptrs[chosen_proxy]->recovery(&recovery_context, recovery_request, &recovery_reply);
      if (status.ok())
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " success!" << std::endl;
        return true;
      }
      else
      {
        std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
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
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
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
            std::cout << "[Coordinator] partial degraded read of " << failed_block_id << " failed!" << std::endl;
          }
        }));

      }
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, &recovery_group_ids](){
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
          std::cout << "[Coordinator] recovery of " << stripe_id << "_" << failed_block_id << " failed!" << std::endl;
        }
      }
      ));
      for(int i = 0; i < threads.size(); i++){
        threads[i].join();
      }
    }
    return true;
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
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
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
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, 
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
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
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
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port](){
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
          degraded_read_request.set_clientport(dest_proxy_port + ECProject::PROXY_PORT_SHIFT);
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
      int cross_rack_num = recovery_group_ids.size() - 1;
      threads.push_back(std::thread([this, &t_stripe, cross_rack_num, dest_group_id, dest_cluster_id, dest_proxy_ip, dest_proxy_port, stripe_id, failed_block_id, client_ip, client_port, block_id](){
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

    double dest_proxy_network_time;
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
    int node_port = m_node_table[node_id].node_port;
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
        if (opp == SET || opp == APPEND)
        {
          m_object_commit_table[key] = m_object_updating_table[key];
          cv.notify_all();
          m_object_updating_table.erase(key);
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
    if (opp == SET || opp == APPEND)
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
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a cluster
              int t_cluster_id = randomly_select_a_cluster(stripe_id);
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
                    g_cluster_id = randomly_select_a_cluster(stripe_id);
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
            g_cluster_id = randomly_select_a_cluster(stripe_id);
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
