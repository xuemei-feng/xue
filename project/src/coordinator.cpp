#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <unistd.h>
#include "lrc.h"
#include <sys/time.h>
#include <chrono>
#include <limits>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <stdexcept>
#include <numeric>
#include <sstream>

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
      return code_type == "AzureLRC" || code_type == "RandomLRC";
    }

    void merge_interval_in_map(std::map<int, std::vector<std::pair<int, int>>> *m, int key, int lo, int hi)
    {
      if (m == nullptr || hi <= lo)
      {
        return;
      }
      std::vector<std::pair<int, int>> &vec = (*m)[key];
      vec.push_back(std::make_pair(lo, hi));
      std::sort(vec.begin(), vec.end());
      std::vector<std::pair<int, int>> merged;
      for (const auto &p : vec)
      {
        if (merged.empty() || p.first > merged.back().second)
        {
          merged.push_back(p);
        }
        else
        {
          merged.back().second = std::max(merged.back().second, p.second);
        }
      }
      vec.swap(merged);
    }

    // RackCU：各数据块在「块内字节坐标」上与校验列对齐；合并所有块上的更新区间（重叠已合并），再按 UnitSize 对齐。
    // 返回若干互不相交的 [offset, size)，size>0，且均在 [0, block_size) 内。
    std::vector<std::pair<int, int>> build_rackcu_parity_slices(
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int unit_size,
        int block_size)
    {
      std::vector<std::pair<int, int>> expanded;
      expanded.reserve(32);
      for (const auto &bp : block_intervals)
      {
        (void)bp.first;
        for (const auto &seg : bp.second)
        {
          const int lo = seg.first;
          const int hi = seg.second;
          if (hi <= lo || lo < 0 || hi > block_size)
          {
            continue;
          }
          const int lo_a = (lo / unit_size) * unit_size;
          int hi_a = ((hi + unit_size - 1) / unit_size) * unit_size;
          if (hi_a > block_size)
          {
            hi_a = block_size;
          }
          if (hi_a <= lo_a)
          {
            continue;
          }
          expanded.emplace_back(lo_a, hi_a);
        }
      }
      if (expanded.empty())
      {
        return {};
      }
      std::sort(expanded.begin(), expanded.end());
      std::vector<std::pair<int, int>> merged;
      for (const auto &p : expanded)
      {
        if (merged.empty() || p.first > merged.back().second)
        {
          merged.push_back(p);
        }
        else
        {
          merged.back().second = std::max(merged.back().second, p.second);
        }
      }
      std::vector<std::pair<int, int>> out;
      for (const auto &m : merged)
      {
        const int off = m.first;
        const int sz = m.second - m.first;
        if (sz > 0 && off >= 0 && off + sz <= block_size)
        {
          out.emplace_back(off, sz);
        }
      }
      return out;
    }

    const Block *find_block_const(const Stripe &stripe, int block_id)
    {
      for (const Block *b : stripe.blocks)
      {
        if (b != nullptr && b->block_id == block_id)
        {
          return b;
        }
      }
      return nullptr;
    }

    std::string gen_rackcu_append_key(int stripe_id, int seq)
    {
      // 与 ToolBox::gen_append_key 格式一致，但使用大基数避免与常规 group id 冲突
      return std::to_string(stripe_id) + "_" + std::to_string(900000 + seq);
    }

    // ReplyProxyIPsPorts.group_ids：与 append_keys 对齐，供 Client::rackcu_update 解析步骤语义
    enum RackCuClientStep : int32_t
    {
      RACKCU_STEP_DATA_HOME = 1,
      RACKCU_STEP_DATA_TO_COLLECTOR = 2,
      RACKCU_STEP_PARITY_GLOBAL = 3,
      RACKCU_STEP_PARITY_GLOBAL_FROM_DATA = 4,
      RACKCU_STEP_LOCAL_PARITY = 5,
    };
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
    (void)stripe;
    throw std::runtime_error("XueLRC placement strategy has been removed");
  }

  void CoordinatorImpl::initialize_random_lrc_stripe_placement(Stripe *stripe)
  {
    // Random placement:
    // 1) 每条带使用 6 个紧邻 cluster（轮询起点，环形取连续 6 个）。
    // 2) 所有本地校验块放到 1 个专用 cluster，所有全局校验块放到另 1 个专用 cluster。
    // 3) 这两个专用 cluster 不放该条带数据块；数据块仅在剩余 4 个 cluster 随机放置。
    // 4) 数据放置约束：每个数据 cluster 的数据块数 <= r + 1。
    Block *blocks_info = new Block[stripe->n];
    assert(stripe->object_keys.size() == 1);

    const int cluster_num = m_sys_config->ClusterNum;
    if (cluster_num <= 0)
    {
      throw std::runtime_error("ClusterNum must be positive for RandomLRC placement");
    }

    const int target_cluster_num = 6;
    if (cluster_num < target_cluster_num)
    {
      throw std::runtime_error("RandomLRC placement requires at least 6 clusters");
    }
    std::vector<int> selected_clusters;
    selected_clusters.reserve(target_cluster_num);
    // 轮询选择紧邻 cluster：以 stripe_id 为起点，按环形连续取 5 个。
    const int start_cluster = stripe->stripe_id % cluster_num;
    for (int i = 0; i < target_cluster_num; ++i)
    {
      selected_clusters.push_back((start_cluster + i) % cluster_num);
    }
    const int local_parity_cluster = selected_clusters[0];
    const int global_parity_cluster = selected_clusters[1];
    std::vector<int> data_clusters = {selected_clusters[2], selected_clusters[3], selected_clusters[4], selected_clusters[5]};

    std::random_device rd;
    std::mt19937 gen(rd());

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

    std::vector<int> data_block_order(stripe->k);
    std::iota(data_block_order.begin(), data_block_order.end(), 0);
    const int max_attempts = 256;
    bool placed = false;
    std::vector<int> assigned_cluster(stripe->n, -1);

    for (int attempt = 0; attempt < max_attempts && !placed; ++attempt)
    {
      std::shuffle(data_block_order.begin(), data_block_order.end(), gen);
      std::fill(assigned_cluster.begin(), assigned_cluster.end(), -1);
      std::map<int, int> cluster_data_block_count;
      bool ok = true;

      // 先固定校验块专用 cluster。
      for (int i = stripe->k; i < stripe->k + stripe->r; ++i)
      {
        assigned_cluster[i] = global_parity_cluster;
      }
      for (int i = stripe->k + stripe->r; i < stripe->n; ++i)
      {
        assigned_cluster[i] = local_parity_cluster;
      }

      // 数据块仅在 4 个数据 cluster 内随机放置。
      for (int block_idx : data_block_order)
      {
        std::vector<int> candidate_clusters = data_clusters;
        std::shuffle(candidate_clusters.begin(), candidate_clusters.end(), gen);
        bool assigned = false;
        for (int cid : candidate_clusters)
        {
          int next_data_cnt = cluster_data_block_count[cid] + 1;
          if (next_data_cnt <= stripe->r + 1)
          {
            assigned_cluster[block_idx] = cid;
            cluster_data_block_count[cid] = next_data_cnt;
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
      else if (code_type == "RandomLRC")
      {
        initialize_random_lrc_stripe_placement(&t_stripe);
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
    int sum_append_size = 0;
    for (const auto &plan : append_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    for (auto &thread : threads)
    {
      thread.join();
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

  grpc::Status CoordinatorImpl::uploadRackCuUpdate(
      grpc::ServerContext *context,
      const coordinator_proto::RackCuUpdateRequest *request,
      coordinator_proto::ReplyProxyIPsPorts *proxyIPPort)
  {
    (void)context;
    if (request == nullptr || proxyIPPort == nullptr)
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "null request/response");
    }
    const std::string code_type = m_sys_config->CodeType;
    if (!is_azure_like_code(code_type))
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "RackCU only supports AzureLRC / RandomLRC (Azure grouping)");
    }

    const int stripe_id = request->stripe_id();
    auto it_stripe = m_stripe_table.find(stripe_id);
    if (it_stripe == m_stripe_table.end())
    {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "stripe_id not found");
    }
    Stripe *stripe = &it_stripe->second;

    if (request->ranges_size() == 0)
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty ranges");
    }

    const int k = stripe->k;
    const int r = stripe->r;
    const int block_size = static_cast<int>(m_sys_config->BlockSize);
    const int unit_size = static_cast<int>(m_sys_config->UnitSize);
    const int stripe_data_bytes = k * block_size;

    std::map<int, std::vector<std::pair<int, int>>> block_intervals;
    for (int ri = 0; ri < request->ranges_size(); ri++)
    {
      const auto &rg = request->ranges(ri);
      const int lo = rg.logical_offset_start();
      const int hi = rg.logical_offset_end();
      if (hi <= lo)
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid logical range (require start < end)");
      }
      if (lo < 0 || hi > stripe_data_bytes)
      {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "logical range out of stripe data address space");
      }
      int pos = lo;
      const int logical_end = hi - 1;
      while (pos <= logical_end)
      {
        const int bid = pos / block_size;
        const int off = pos % block_size;
        const int tail = block_size - off;
        const int len = std::min(tail, logical_end - pos + 1);
        merge_interval_in_map(&block_intervals, bid, off, off + len);
        pos += len;
      }
    }

    if (block_intervals.empty())
    {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "no data blocks affected");
    }

    std::cout << "[RackCU] stripe " << stripe_id << " updated blocks/segments:";
    for (const auto &bp : block_intervals)
    {
      std::cout << " b" << bp.first << "{";
      for (size_t si = 0; si < bp.second.size(); si++)
      {
        std::cout << "[" << bp.second[si].first << "," << bp.second[si].second << ")";
        if (si + 1 < bp.second.size())
        {
          std::cout << ",";
        }
      }
      std::cout << "}";
    }
    std::cout << std::endl;

    const std::vector<std::pair<int, int>> parity_slices = build_rackcu_parity_slices(block_intervals, unit_size, block_size);
    if (parity_slices.empty())
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "invalid parity slice derived from ranges");
    }

    std::vector<int> touched_data_blocks;
    touched_data_blocks.reserve(block_intervals.size());
    for (const auto &bp : block_intervals)
    {
      touched_data_blocks.push_back(bp.first);
    }
    std::sort(touched_data_blocks.begin(), touched_data_blocks.end());

    auto count_updated_data_blocks_for_cluster = [&](int cluster_id) -> int
    {
      int cnt = 0;
      for (int dbid : touched_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        if (db != nullptr && db->map2cluster == cluster_id)
        {
          cnt++;
        }
      }
      return cnt;
    };
    auto count_global_parity_blocks_for_cluster = [&](int cluster_id) -> int
    {
      int cnt = 0;
      for (int pj = 0; pj < r; pj++)
      {
        const Block *pb = find_block_const(*stripe, k + pj);
        if (pb != nullptr && pb->map2cluster == cluster_id)
        {
          cnt++;
        }
      }
      return cnt;
    };
    auto collector_score_for_cluster = [&](int cluster_id) -> int
    {
      return count_updated_data_blocks_for_cluster(cluster_id) + count_global_parity_blocks_for_cluster(cluster_id);
    };

    std::unordered_set<int> local_parity_clusters;
    for (int g = 0; g < stripe->num_groups; g++)
    {
      const int lbid = k + r + g;
      const Block *lb = find_block_const(*stripe, lbid);
      if (lb != nullptr)
      {
        local_parity_clusters.insert(lb->map2cluster);
      }
    }
    auto has_touched_data_block_on_cluster = [&](int cluster_id) -> bool
    {
      for (int dbid : touched_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        if (db != nullptr && db->map2cluster == cluster_id)
        {
          return true;
        }
      }
      return false;
    };

    std::vector<int> collector_candidates;
    collector_candidates.reserve(stripe->place2clusters.size());
    for (int cid : stripe->place2clusters)
    {
      if (local_parity_clusters.count(cid) == 0)
      {
        collector_candidates.push_back(cid);
      }
    }
    // 极端情况下（本地校验覆盖了全部 cluster），回退到原全集，避免无候选。
    if (collector_candidates.empty())
    {
      collector_candidates.assign(stripe->place2clusters.begin(), stripe->place2clusters.end());
    }

    int best_cluster = -1;
    int best_cnt = -1;
    for (int cid : collector_candidates)
    {
      const int c = collector_score_for_cluster(cid);
      if (c > best_cnt)
      {
        best_cnt = c;
        best_cluster = cid;
        continue;
      }
      if (c == best_cnt)
      {
        const bool curr_has_data = has_touched_data_block_on_cluster(cid);
        const bool best_has_data = has_touched_data_block_on_cluster(best_cluster);
        if ((curr_has_data && !best_has_data) || (curr_has_data == best_has_data && (best_cluster < 0 || cid < best_cluster)))
        {
          best_cluster = cid;
        }
      }
    }
    if (best_cluster < 0)
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "failed to select collector cluster");
    }

    std::cout << "[RackCU] stripe=" << stripe_id << " collector_cluster=" << best_cluster
              << " collector_update_blocks=" << best_cnt << " parity_slice_components=" << parity_slices.size();
    for (size_t psi = 0; psi < parity_slices.size(); psi++)
    {
      std::cout << " [" << parity_slices[psi].first << "," << (parity_slices[psi].first + parity_slices[psi].second) << ")";
    }
    std::cout << std::endl;

    std::vector<proxy_proto::AppendStripeDataPlacement> plans;
    std::vector<int32_t> plan_steps;
    plans.reserve(16);
    plan_steps.reserve(16);
    int append_seq = 0;

    auto emit_plan = [&](proxy_proto::AppendStripeDataPlacement plan, int32_t step) {
      if (plan.append_mode().empty())
      {
        plan.set_append_mode(m_sys_config->AppendMode);
      }
      plans.push_back(std::move(plan));
      plan_steps.push_back(step);
    };
    auto describe_data_slices = [&](const std::vector<int> &dbids) -> std::string
    {
      std::ostringstream oss;
      bool first_block = true;
      for (int dbid : dbids)
      {
        auto it = block_intervals.find(dbid);
        if (it == block_intervals.end())
        {
          continue;
        }
        if (!first_block)
        {
          oss << ";";
        }
        first_block = false;
        oss << "b" << dbid << ":";
        bool first_seg = true;
        for (const auto &seg : it->second)
        {
          if (!first_seg)
          {
            oss << ",";
          }
          first_seg = false;
          oss << "[" << seg.first << "," << seg.second << ")";
        }
      }
      if (first_block)
      {
        return "none";
      }
      return oss.str();
    };

    // 1) 各数据块所在 cluster 写入本次更新增量
    std::map<int, std::vector<int>> data_blocks_by_cluster;
    for (int dbid : touched_data_blocks)
    {
      const Block *db = find_block_const(*stripe, dbid);
      if (db == nullptr)
      {
        return grpc::Status(grpc::StatusCode::INTERNAL, "missing data block metadata");
      }
      data_blocks_by_cluster[db->map2cluster].push_back(dbid);
    }
    for (auto &kv : data_blocks_by_cluster)
    {
      std::sort(kv.second.begin(), kv.second.end());
    }

    for (const auto &kv : data_blocks_by_cluster)
    {
      const int cluster_id = kv.first;
      std::cout << "[RackCU][Transfer] client -> c" << cluster_id
                << " content=data_deltas"
                << " blocks=" << kv.second.size()
                << " detail=" << describe_data_slices(kv.second) << std::endl;
      proxy_proto::AppendStripeDataPlacement plan;
      plan.set_key(gen_rackcu_append_key(stripe_id, append_seq++));
      plan.set_stripe_id(stripe_id);
      plan.set_cluster_id(cluster_id);
      plan.set_is_merge_parity(false);
      plan.set_is_serialized(true);
      size_t append_size = 0;
      for (int dbid : kv.second)
      {
        const Block *db = find_block_const(*stripe, dbid);
        const Node &node = m_node_table[db->map2node];
        for (const auto &seg : block_intervals[dbid])
        {
          const int len = seg.second - seg.first;
          append_size += static_cast<size_t>(len);
          addBlockToAppendPlan(plan, db, node, std::make_pair(len, seg.first));
        }
      }
      plan.set_append_size(append_size);
      // home Proxy：写新数据前读盘 old 并记录 ΔD=new⊕old（见 proxy RACKCU_DATA_HOME_XOR_FIRST）
      plan.set_append_mode("RACKCU_DATA_HOME_XOR_FIRST");
      emit_plan(std::move(plan), RACKCU_STEP_DATA_HOME);
    }

    // 2) 将不在 collector 所在 cluster 的更新数据块增量发往 collector（顺序与 touched_data_blocks 一致）
    {
      std::vector<int> remote_touched_data_blocks;
      remote_touched_data_blocks.reserve(touched_data_blocks.size());
      for (int dbid : touched_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        if (db != nullptr && db->map2cluster != best_cluster)
        {
          remote_touched_data_blocks.push_back(dbid);
        }
      }
      if (!remote_touched_data_blocks.empty())
      {
      std::cout << "[RackCU][Transfer] data-clusters -> collector c" << best_cluster
                << " content=data_deltas"
                << " blocks=" << remote_touched_data_blocks.size()
                << " detail=" << describe_data_slices(remote_touched_data_blocks) << std::endl;
      proxy_proto::AppendStripeDataPlacement plan;
      plan.set_key(gen_rackcu_append_key(stripe_id, append_seq++));
      plan.set_stripe_id(stripe_id);
      plan.set_cluster_id(best_cluster);
      plan.set_is_merge_parity(false);
      plan.set_is_serialized(true);
      size_t append_size = 0;
      for (int dbid : remote_touched_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        const Node &node = m_node_table[db->map2node];
        for (const auto &seg : block_intervals[dbid])
        {
          const int len = seg.second - seg.first;
          append_size += static_cast<size_t>(len);
          addBlockToAppendPlan(plan, db, node, std::make_pair(len, seg.first));
        }
      }
      plan.set_append_size(append_size);
      emit_plan(std::move(plan), RACKCU_STEP_DATA_TO_COLLECTOR);
      }
    }

    for (size_t ps_idx = 0; ps_idx < parity_slices.size(); ps_idx++)
    {
      const int parity_slice_offset = parity_slices[ps_idx].first;
      const int parity_slice_size = parity_slices[ps_idx].second;

      // 3) 全局校验：按 cluster 比较 collector 与 global parity cluster 的“待更新块数”
      for (int pj = 0; pj < r; pj++)
      {
        const int pbid = k + pj;
        const Block *pb = find_block_const(*stripe, pbid);
        if (pb == nullptr)
        {
          return grpc::Status(grpc::StatusCode::INTERNAL, "missing global parity block metadata");
        }
        const int g_cluster = pb->map2cluster;
        int global_parity_cnt_on_gcluster = 0;
        for (int gp = 0; gp < r; gp++)
        {
          const Block *gb = find_block_const(*stripe, k + gp);
          if (gb != nullptr && gb->map2cluster == g_cluster)
          {
            global_parity_cnt_on_gcluster++;
          }
        }
        const bool send_parity_from_collector = (best_cnt >= global_parity_cnt_on_gcluster);

        if (send_parity_from_collector)
        {
          std::cout << "[RackCU][Transfer] collector c" << best_cluster << " -> global-parity-cluster c"
                    << g_cluster
                    << " content=global_parity_delta"
                    << " target_block=g" << pj
                    << " parity_comp=" << ps_idx << "/" << parity_slices.size()
                    << " slice=[" << parity_slice_offset << "," << (parity_slice_offset + parity_slice_size) << ")"
                    << " data_detail=" << describe_data_slices(touched_data_blocks)
                    << " parity_delta_bytes=" << parity_slice_size
                    << std::endl;
          proxy_proto::AppendStripeDataPlacement plan;
          plan.set_key(gen_rackcu_append_key(stripe_id, append_seq++));
          plan.set_stripe_id(stripe_id);
          // 由 collector proxy 先计算全局校验增量，再跨 cluster 写入目标全局校验块。
          plan.set_cluster_id(best_cluster);
          plan.set_is_merge_parity(true);
          plan.set_is_serialized(true);
          plan.set_append_mode("RACKCU_PARITY_GLOBAL_BY_COLLECTOR_HOME_DELTA");
          plan.set_target_proxy_ip(m_cluster_table[g_cluster].proxy_ip);
          plan.set_target_proxy_port(m_cluster_table[g_cluster].proxy_port);
          size_t append_size = 0;
          for (int dbid : touched_data_blocks)
          {
            const Block *db = find_block_const(*stripe, dbid);
            const Node &dnode = m_node_table[db->map2node];
            for (const auto &seg : block_intervals[dbid])
            {
              const int len = seg.second - seg.first;
              append_size += static_cast<size_t>(len);
              addBlockToAppendPlan(plan, db, dnode, std::make_pair(len, seg.first));
            }
          }
          const Node &node = m_node_table[pb->map2node];
          // 尾部附带目标全局校验块的元信息，客户端发送对应占位字节。
          addBlockToAppendPlan(plan, pb, node, std::make_pair(parity_slice_size, parity_slice_offset));
          append_size += static_cast<size_t>(parity_slice_size);
          plan.set_append_size(append_size);
          emit_plan(std::move(plan), RACKCU_STEP_PARITY_GLOBAL);
        }
        else
        {
          std::cout << "[RackCU][Transfer] data-clusters -> global-parity-cluster c" << g_cluster
                    << " content=data_deltas(+parity_target_meta)"
                    << " target_block=g" << pj
                    << " data_blocks=" << touched_data_blocks.size()
                    << " parity_comp=" << ps_idx << "/" << parity_slices.size()
                    << " parity_slice=[" << parity_slice_offset << "," << (parity_slice_offset + parity_slice_size) << ")"
                    << " data_detail=" << describe_data_slices(touched_data_blocks)
                    << " parity_meta_bytes=" << parity_slice_size
                    << std::endl;
          proxy_proto::AppendStripeDataPlacement plan;
          plan.set_key(gen_rackcu_append_key(stripe_id, append_seq++));
          plan.set_stripe_id(stripe_id);
          plan.set_cluster_id(g_cluster);
          // 发送数据增量到全局校验块所在 cluster，由目标 proxy 完成全局校验增量合并。
          plan.set_is_merge_parity(true);
          plan.set_is_serialized(true);
          plan.set_append_mode("RACKCU_GLOBAL_FROM_DATA_HOME_DELTA");
          plan.set_target_proxy_ip(m_cluster_table[g_cluster].proxy_ip);
          plan.set_target_proxy_port(m_cluster_table[g_cluster].proxy_port);
          size_t append_size = 0;
          const Node &pnode = m_node_table[pb->map2node];
          for (int dbid : touched_data_blocks)
          {
            const Block *db = find_block_const(*stripe, dbid);
            const Node &dnode = m_node_table[db->map2node];
            for (const auto &seg : block_intervals[dbid])
            {
              const int len = seg.second - seg.first;
              append_size += static_cast<size_t>(len);
              addBlockToAppendPlan(plan, db, dnode, std::make_pair(len, seg.first));
            }
          }
          // 尾部附带 parity 目标块元信息（offset/size），客户端发送对应占位字节。
          addBlockToAppendPlan(plan, pb, pnode, std::make_pair(parity_slice_size, parity_slice_offset));
          append_size += static_cast<size_t>(parity_slice_size);
          plan.set_append_size(append_size);
          emit_plan(std::move(plan), RACKCU_STEP_PARITY_GLOBAL_FROM_DATA);
        }
      }

    }

    // 4) 本地校验：按“本地组独立 parity slice”生成计划；同组同 cluster 合并，不同组分开发送。
    std::unordered_set<int> local_groups;
    for (int dbid : touched_data_blocks)
    {
      const Block *db = find_block_const(*stripe, dbid);
      local_groups.insert(db->map2group);
    }
    std::vector<int> local_groups_sorted(local_groups.begin(), local_groups.end());
    std::sort(local_groups_sorted.begin(), local_groups_sorted.end());
    for (int g : local_groups_sorted)
    {
      std::vector<int> group_data_blocks;
      std::map<int, std::vector<std::pair<int, int>>> group_block_intervals;
      for (int dbid : touched_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        if (db != nullptr && db->map2group == g)
        {
          group_data_blocks.push_back(dbid);
          group_block_intervals[dbid] = block_intervals[dbid];
        }
      }
      if (group_data_blocks.empty())
      {
        continue;
      }
      std::sort(group_data_blocks.begin(), group_data_blocks.end());
      const int lbid = k + r + g;
      const Block *lp = find_block_const(*stripe, lbid);
      if (lp == nullptr)
      {
        return grpc::Status(grpc::StatusCode::INTERNAL, "missing local parity block metadata");
      }
      const int target_cluster = lp->map2cluster;
      std::map<int, std::vector<int>> group_data_blocks_by_cluster;
      for (int dbid : group_data_blocks)
      {
        const Block *db = find_block_const(*stripe, dbid);
        if (db == nullptr)
        {
          return grpc::Status(grpc::StatusCode::INTERNAL, "missing local-group data block metadata");
        }
        group_data_blocks_by_cluster[db->map2cluster].push_back(dbid);
      }
      for (auto &kv : group_data_blocks_by_cluster)
      {
        std::sort(kv.second.begin(), kv.second.end());
      }

      for (const auto &kv : group_data_blocks_by_cluster)
      {
        const int source_cluster = kv.first;
        const std::vector<int> &src_group_blocks = kv.second;
        std::map<int, std::vector<std::pair<int, int>>> src_block_intervals;
        for (int dbid : src_group_blocks)
        {
          src_block_intervals[dbid] = block_intervals[dbid];
        }
        const std::vector<std::pair<int, int>> src_parity_slices =
            build_rackcu_parity_slices(src_block_intervals, unit_size, block_size);
        if (src_parity_slices.empty())
        {
          return grpc::Status(grpc::StatusCode::INTERNAL, "invalid local parity slice derived from source-cluster ranges");
        }

        for (size_t gsi = 0; gsi < src_parity_slices.size(); gsi++)
        {
          const int parity_slice_offset = src_parity_slices[gsi].first;
          const int parity_slice_size = src_parity_slices[gsi].second;
          std::cout << "[RackCU][Transfer] local-group " << g << " source-cluster c" << source_cluster
                    << " -> local-parity-cluster c" << target_cluster
                    << " content=local_data_deltas(+parity_target_meta)"
                    << " target_block=l" << g
                    << " group_parity_comp=" << gsi << "/" << src_parity_slices.size()
                    << " slice=[" << parity_slice_offset << "," << (parity_slice_offset + parity_slice_size) << ")"
                    << " data_detail=" << describe_data_slices(src_group_blocks)
                    << " parity_meta_bytes=" << parity_slice_size
                    << std::endl;

          proxy_proto::AppendStripeDataPlacement plan;
          plan.set_key(gen_rackcu_append_key(stripe_id, append_seq++));
          plan.set_stripe_id(stripe_id);
          plan.set_cluster_id(source_cluster);
          plan.set_is_merge_parity(true);
          plan.set_is_serialized(true);
          plan.set_append_mode("RACKCU_LOCAL_FROM_DATA_HOME_DELTA");
          plan.set_target_proxy_ip(m_cluster_table[target_cluster].proxy_ip);
          plan.set_target_proxy_port(m_cluster_table[target_cluster].proxy_port);
          size_t append_size = 0;
          const Node &pnode = m_node_table[lp->map2node];

          for (int dbid : src_group_blocks)
          {
            const Block *db = find_block_const(*stripe, dbid);
            const Node &dnode = m_node_table[db->map2node];
            for (const auto &seg : block_intervals[dbid])
            {
              const int len = seg.second - seg.first;
              append_size += static_cast<size_t>(len);
              addBlockToAppendPlan(plan, db, dnode, std::make_pair(len, seg.first));
            }
          }
          addBlockToAppendPlan(plan, lp, pnode, std::make_pair(parity_slice_size, parity_slice_offset));
          append_size += static_cast<size_t>(parity_slice_size);
          plan.set_append_size(append_size);
          emit_plan(std::move(plan), RACKCU_STEP_LOCAL_PARITY);
        }
      }
    }

    if (plans.size() != plan_steps.size())
    {
      return grpc::Status(grpc::StatusCode::INTERNAL, "RackCU internal plan/step mismatch");
    }

    for (const auto &plan : plans)
    {
      m_mutex.lock();
      m_object_commit_table.erase(plan.key());
      m_mutex.unlock();
    }

    uint64_t sum_append_size = 0;
    std::vector<std::thread> threads;
    threads.reserve(plans.size());
    for (size_t i = 0; i < plans.size(); i++)
    {
      threads.emplace_back(&CoordinatorImpl::notify_proxies_ready, this, plans[i]);
      proxyIPPort->add_append_keys(plans[i].key());
      proxyIPPort->add_proxyips(m_cluster_table[plans[i].cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plans[i].cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT);
      proxyIPPort->add_cluster_slice_sizes(plans[i].append_size());
      proxyIPPort->add_group_ids(plan_steps[i]);
      std::string serialized;
      if (!plans[i].SerializeToString(&serialized))
      {
        return grpc::Status(grpc::StatusCode::INTERNAL, "failed to serialize RackCU append plan");
      }
      proxyIPPort->add_append_plans(serialized);
      sum_append_size += plans[i].append_size();
    }
    for (auto &t : threads)
    {
      t.join();
    }
    proxyIPPort->set_sum_append_size(sum_append_size);

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
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "RandomLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or RandomLRC!");

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
    else if (code_type == "RandomLRC")
    {
      initialize_random_lrc_stripe_placement(&t_stripe);
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
    size_t sum_append_size = 0;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      sum_append_size += plan.append_size();
    }
    for (auto &thread : threads)
    {
      thread.join();
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
    assert((code_type == "UniLRC" || code_type == "AzureLRC" || code_type == "OptimalLRC" || code_type == "UniformLRC" || code_type == "RandomLRC") && "Error: code type must be UniLRC, AzureLRC, OptimalLRC, UniformLRC, or RandomLRC!");

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
    else if (code_type == "RandomLRC")
    {
      initialize_random_lrc_stripe_placement(&t_stripe);
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
    size_t sum_append_size = 0;
    for (const auto &plan : add_plans)
    {
      threads.push_back(std::thread(&CoordinatorImpl::notify_proxies_ready, this, plan));
      proxyIPPort->add_append_keys(plan.key());
      proxyIPPort->add_proxyips(m_cluster_table[plan.cluster_id()].proxy_ip);
      proxyIPPort->add_proxyports(m_cluster_table[plan.cluster_id()].proxy_port + ECProject::PROXY_PORT_SHIFT); // use another port to accept data
      proxyIPPort->add_cluster_slice_sizes(plan.append_size());
      //proxyIPPort->add_group_ids(group_id);
      sum_append_size += plan.append_size();
      //group_id++;
    }
    for (auto &thread : threads)
    {
      thread.join();
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
      if (commit_abortkey->rackcu_home_delta_blob().size() > 0)
      {
        m_rackcu_home_delta_by_append_key[key] = commit_abortkey->rackcu_home_delta_blob();
      }
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
    auto rd_it = m_rackcu_home_delta_by_append_key.find(key);
    if (rd_it != m_rackcu_home_delta_by_append_key.end())
    {
      reply->set_rackcu_home_delta_blob(rd_it->second);
      m_rackcu_home_delta_by_append_key.erase(rd_it);
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
