#include "proxy.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include <algorithm>
#include <thread>
#include <cassert>
#include <string>
#include <fstream>
#include <sys/mman.h>
#include "unilrc_encoder.h"
#include <chrono>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <memory>
#include <mutex>
#include <cstring>
#include <ctime>
#include <limits>
#include <iomanip>
#include <sstream>
template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};
namespace ECProject
{
  static std::string cord_dbg_hex_preview(const void *data, size_t len, size_t max_show = 48)
  {
    if (!data || len == 0)
      return "";
    const auto *p = static_cast<const unsigned char *>(data);
    const size_t n = std::min(len, max_show);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < n; ++i)
      oss << std::setw(2) << static_cast<unsigned>(p[i]);
    if (len > max_show)
      oss << "...+" << (len - max_show) << "b";
    return oss.str();
  }

  /** 校验增量缓冲区中非零字节的最紧外包 [poff, poff+plen)；全零则 plen=0（可跳过传输/磁盘 XOR）。 */
  static std::pair<size_t, size_t> cord_parity_delta_nonzero_span(const char *buf, size_t n)
  {
    if (buf == nullptr || n == 0)
      return {0, 0};
    size_t lo = 0;
    while (lo < n && static_cast<unsigned char>(buf[lo]) == 0)
      ++lo;
    if (lo == n)
      return {0, 0};
    size_t hi = n;
    while (hi > lo && static_cast<unsigned char>(buf[hi - 1]) == 0)
      --hi;
    return {lo, hi - lo};
  }

  static std::mutex g_cord_xfer_mu;
  static std::map<std::string, std::vector<uint8_t>> g_cord_collector_xor_acc;
  static std::map<std::string, std::vector<uint8_t>> g_cord_mst_stream;
  static std::map<std::string, std::vector<uint8_t>> g_cord_collector_block_delta;
  static std::map<std::string, std::vector<std::vector<uint8_t>>> g_cord_collector_parity_coded;
  static std::mutex g_cord_plan_reg_mu;
  static std::map<std::string, std::shared_ptr<const proxy_proto::CordTransferPlan>> g_cord_plans_by_key;
  static std::mutex g_cord_plan_exec_mu;
  static std::unordered_map<std::string, std::thread> g_cord_plan_exec_threads;

  static std::string cord_plan_wall_ts_ms();

  /** 单 plan_key：纯传输窗口（peer stub 预热后步骤循环起点 → 本 proxy 发送步骤结束且入站 parity 写盘+校验读完成）。 */
  struct CordPureXferTracker {
    std::mutex mu;
    bool pure_xfer_loop_started = false;
    std::chrono::steady_clock::time_point pure_xfer_t0{};
    bool have_xfer_wall_t0 = false;
    std::chrono::system_clock::time_point xfer_wall_t0{};
    int parity_dn_write_inflight = 0;
    std::chrono::steady_clock::time_point last_parity_dn_verified{};
    bool have_last_parity_dn_verified = false;
    bool have_last_parity_wall_verified = false;
    std::chrono::system_clock::time_point last_parity_wall_verified{};
  };
  static std::mutex g_cord_pure_xfer_mu;
  static std::unordered_map<std::string, std::shared_ptr<CordPureXferTracker>> g_cord_pure_xfer_trackers;

  struct CordJoinXferTimingSample {
    double pure_xfer_sec = 0.;
    int64_t wall_start_unix_ms = 0;
    int64_t wall_end_unix_ms = 0;
  };
  static std::mutex g_cord_join_xfer_timing_mu;
  static std::unordered_map<std::string, CordJoinXferTimingSample> g_cord_join_xfer_timing_by_plan;

  static int64_t cord_sys_clock_to_unix_ms(std::chrono::system_clock::time_point tp)
  {
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
  }


  static std::shared_ptr<CordPureXferTracker> cord_pure_xfer_tracker_ptr(const std::string &pk)
  {
    std::lock_guard<std::mutex> lk(g_cord_pure_xfer_mu);
    auto &p = g_cord_pure_xfer_trackers[pk];
    if (!p)
      p = std::make_shared<CordPureXferTracker>();
    return p;
  }

  static void cord_pure_xfer_erase_tracker(const std::string &pk)
  {
    std::lock_guard<std::mutex> lk(g_cord_pure_xfer_mu);
    g_cord_pure_xfer_trackers.erase(pk);
  }

  static void cord_pure_xfer_note_loop_start(const std::string &pk)
  {
    auto tr = cord_pure_xfer_tracker_ptr(pk);
    const auto now = std::chrono::steady_clock::now();
    const auto wall_now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> lk(tr->mu);
    if (!tr->pure_xfer_loop_started)
    {
      tr->pure_xfer_loop_started = true;
      tr->pure_xfer_t0 = now;
      tr->xfer_wall_t0 = wall_now;
      tr->have_xfer_wall_t0 = true;
    }
  }

  static void cord_pure_xfer_parity_dn_begin(const std::string &pk)
  {
    auto tr = cord_pure_xfer_tracker_ptr(pk);
    std::lock_guard<std::mutex> lk(tr->mu);
    ++tr->parity_dn_write_inflight;
  }

  static void cord_pure_xfer_parity_dn_abort(const std::string &pk)
  {
    auto tr = cord_pure_xfer_tracker_ptr(pk);
    std::lock_guard<std::mutex> lk(tr->mu);
    if (tr->parity_dn_write_inflight > 0)
      --tr->parity_dn_write_inflight;
  }

  static void cord_pure_xfer_parity_dn_done_verified(const std::string &pk)
  {
    auto tr = cord_pure_xfer_tracker_ptr(pk);
    const auto now = std::chrono::steady_clock::now();
    const auto wall_now = std::chrono::system_clock::now();
    std::lock_guard<std::mutex> lk(tr->mu);
    tr->last_parity_dn_verified = now;
    tr->have_last_parity_dn_verified = true;
    tr->last_parity_wall_verified = wall_now;
    tr->have_last_parity_wall_verified = true;
    if (tr->parity_dn_write_inflight > 0)
      --tr->parity_dn_write_inflight;
  }

  /** 步骤循环结束后调用：等待本机入站 parity 写盘+校验读收尾，再打 pure_xfer 日志，并发布 join 回报样本。 */
  static void cord_pure_xfer_log_after_sender_loop(const std::string &pk, const std::string &proxy_tag,
                                                   std::chrono::steady_clock::time_point sender_loop_done,
                                                   std::chrono::system_clock::time_point sender_wall_done)
  {
    auto tr = cord_pure_xfer_tracker_ptr(pk);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(30);
    for (;;)
    {
      int inflight = 0;
      {
        std::lock_guard<std::mutex> lk(tr->mu);
        inflight = tr->parity_dn_write_inflight;
      }
      if (inflight <= 0 || std::chrono::steady_clock::now() >= deadline)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    int inflight_left = 0;
    bool loop_started = false;
    double pure_sec = -1.;
    std::chrono::steady_clock::time_point t_end = sender_loop_done;
    std::chrono::system_clock::time_point wall_start{};
    std::chrono::system_clock::time_point wall_end = sender_wall_done;
    {
      std::lock_guard<std::mutex> lk(tr->mu);
      inflight_left = tr->parity_dn_write_inflight;
      loop_started = tr->pure_xfer_loop_started;
      if (loop_started)
      {
        t_end = sender_loop_done;
        if (tr->have_last_parity_dn_verified && tr->last_parity_dn_verified > t_end)
          t_end = tr->last_parity_dn_verified;
        pure_sec = std::chrono::duration<double>(t_end - tr->pure_xfer_t0).count();
        wall_end = sender_wall_done;
        if (tr->have_last_parity_wall_verified && tr->last_parity_wall_verified > wall_end)
          wall_end = tr->last_parity_wall_verified;
        wall_start = tr->have_xfer_wall_t0 ? tr->xfer_wall_t0 : sender_wall_done;
      }
    }
    std::ostringstream ob;
    ob << "[CoRD-PLAN][" << cord_plan_wall_ts_ms() << "][" << proxy_tag
       << "] pure_xfer_wall_sec="; // 本机：stub 预热后进入步骤循环 → 步骤循环结束且本机 parity DN 写+校验读收尾
    if (loop_started && pure_sec >= 0.)
      ob << pure_sec;
    else
      ob << "n/a";
    ob << " plan_key=" << pk << " parity_dn_inflight_left=" << inflight_left;
    if (inflight_left > 0)
      ob << " WARN_timeout_waiting_inflight";
    // std::cout << ob.str() << std::endl;  // [CoRD-PLAN] pure_xfer timing (muted)

    if (loop_started && pure_sec >= 0.)
    {
      CordJoinXferTimingSample samp;
      samp.pure_xfer_sec = pure_sec;
      samp.wall_start_unix_ms = cord_sys_clock_to_unix_ms(wall_start);
      samp.wall_end_unix_ms = cord_sys_clock_to_unix_ms(wall_end);
      std::lock_guard<std::mutex> lk(g_cord_join_xfer_timing_mu);
      g_cord_join_xfer_timing_by_plan[pk] = samp;
    }
  }

  static std::string cord_collector_acc_key(const std::string &plan_key, int group_index, int collector_block_id)
  {
    return plan_key + ":" + std::to_string(group_index) + ":" + std::to_string(collector_block_id);
  }

  static std::string cord_collector_block_buf_key(const std::string &plan_key, int group_index, int collector_block_id,
                                                   int src_data_block_id)
  {
    return plan_key + ":" + std::to_string(group_index) + ":" + std::to_string(collector_block_id) + ":db:" +
           std::to_string(src_data_block_id);
  }

  static std::string cord_collector_parity_cache_key(const std::string &plan_key, int group_index,
                                                     int collector_block_id, int parity_ingest_stripe_group)
  {
    return plan_key + ":" + std::to_string(group_index) + ":" + std::to_string(collector_block_id) + ":pig:" +
           std::to_string(parity_ingest_stripe_group);
  }

  static int cord_plan_data_block_stripe_group(const proxy_proto::CordTransferPlan &plan, int data_block_id)
  {
    for (int i = 0; i < plan.cord_block_stripe_groups_size(); ++i)
    {
      if (plan.cord_block_stripe_groups(i).block_id() == data_block_id)
        return plan.cord_block_stripe_groups(i).stripe_group();
    }
    return -999999;
  }

  static std::vector<std::pair<int, int>> cord_plan_sorted_segs_for_block(const proxy_proto::CordTransferPlan &plan,
                                                                          int bid)
  {
    std::vector<std::pair<int, int>> out;
    for (int i = 0; i < plan.cord_block_delta_segs_size(); ++i)
    {
      const auto &s = plan.cord_block_delta_segs(i);
      if (s.block_id() != bid)
        continue;
      if (s.hi_excl() <= s.lo())
        continue;
      out.emplace_back(s.lo(), s.hi_excl());
    }
    std::sort(out.begin(), out.end());
    return out;
  }

  /** strip 轴上的逻辑字节是否在块更新区间内；若在则返回 ingest 缓冲区内 packed 下标，否则 -1。 */
  static int cord_logical_strip_to_packed_delta_idx(const std::vector<std::pair<int, int>> &segs_sorted,
                                                    int strip_logical)
  {
    int packed = 0;
    for (const auto &seg : segs_sorted)
    {
      if (strip_logical < seg.first)
        return -1;
      if (strip_logical < seg.second)
        return packed + (strip_logical - seg.first);
      packed += seg.second - seg.first;
    }
    return -1;
  }

  static int cord_plan_parity_group_hull_lo(const proxy_proto::CordTransferPlan &plan,
                                            int parity_ingest_stripe_group)
  {
    int lo = std::numeric_limits<int>::max();
    for (int i = 0; i < plan.cord_block_delta_segs_size(); ++i)
    {
      const auto &s = plan.cord_block_delta_segs(i);
      if (cord_plan_data_block_stripe_group(plan, s.block_id()) != parity_ingest_stripe_group)
        continue;
      lo = std::min(lo, s.lo());
    }
    return lo == std::numeric_limits<int>::max() ? 0 : lo;
  }

  /** 仅针对 parity_merge_data_block_ids 子集取 hull_lo（与 coordinator 合并校验跨度一致）。 */
  static int cord_plan_merge_subset_hull_lo(const proxy_proto::CordTransferPlan &plan,
                                            const proxy_proto::CordTransferStep &parity_st)
  {
    int lo = std::numeric_limits<int>::max();
    for (int i = 0; i < parity_st.parity_merge_data_block_ids_size(); ++i)
    {
      const int bid = parity_st.parity_merge_data_block_ids(i);
      const std::vector<std::pair<int, int>> segs = cord_plan_sorted_segs_for_block(plan, bid);
      for (const auto &pr : segs)
        lo = std::min(lo, pr.first);
    }
    return lo == std::numeric_limits<int>::max() ? 0 : lo;
  }

  /** 收集器 ingress expect 中源块在 cord_block_delta_segs 上的最小 lo（与 STAR_DATA XOR 下标一致）。 */
  static int cord_plan_collector_expect_src_hull_lo(const proxy_proto::CordTransferPlan &plan, int group_index,
                                                     int collector_block_id)
  {
    const proxy_proto::CordCollectorIngressExpect *ex = nullptr;
    for (int i = 0; i < plan.cord_collector_expects_size(); ++i)
    {
      if (plan.cord_collector_expects(i).group_index() == group_index &&
          plan.cord_collector_expects(i).collector_block_id() == collector_block_id)
      {
        ex = &plan.cord_collector_expects(i);
        break;
      }
    }
    if (ex == nullptr)
      return 0;
    int lo = std::numeric_limits<int>::max();
    for (int i = 0; i < ex->src_data_block_ids_size(); ++i)
    {
      const std::vector<std::pair<int, int>> segs = cord_plan_sorted_segs_for_block(plan, ex->src_data_block_ids(i));
      for (const auto &pr : segs)
        lo = std::min(lo, pr.first);
    }
    return lo == std::numeric_limits<int>::max() ? 0 : lo;
  }

  /**
   * 本步 parity 链路 payload 中 offset=0 对应的块内逻辑条带起点（与 coordinator merged_delta_hull / XOR acc 对齐）。
   * - 有 parity_ingest：与 cord_filtered_xor_parity_chunk 的 group_hull_lo 一致。
   * - 无 parity_ingest（collector_xor_acc）：优先 merge 子集；否则用 collector expect 源块的 hull lo（避免 parity_merge 字段缺失时误用 0）。
   */
  static int cord_plan_parity_payload_abs_lo(const proxy_proto::CordTransferPlan &plan,
                                              const proxy_proto::CordTransferStep &st)
  {
    const bool have_segs = plan.cord_block_delta_segs_size() > 0;
    const bool use_merge = st.parity_merge_data_block_ids_size() > 0;
    if (st.has_parity_ingest_stripe_group())
    {
      if (!have_segs)
        return 0;
      return use_merge ? cord_plan_merge_subset_hull_lo(plan, st)
                       : cord_plan_parity_group_hull_lo(plan, st.parity_ingest_stripe_group());
    }
    if (use_merge && have_segs)
      return cord_plan_merge_subset_hull_lo(plan, st);
    return cord_plan_collector_expect_src_hull_lo(plan, st.group_index(), st.src_block_id());
  }

  static bool cord_filtered_xor_parity_chunk(const proxy_proto::CordTransferPlan &plan, int alg2_group,
                                             int collector_block_id, int parity_ingest_stripe_group,
                                             const proxy_proto::CordTransferStep &parity_st,
                                             uint64_t chunk_off, size_t chunk_len, char *buf_out)
  {
    std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
    std::memset(buf_out, 0, chunk_len);
    const int kblk = plan.k_datablock();
    const bool have_segs = plan.cord_block_delta_segs_size() > 0;
    const bool use_merge_subset = parity_st.parity_merge_data_block_ids_size() > 0;
    const int group_hull_lo =
        have_segs ? (use_merge_subset ? cord_plan_merge_subset_hull_lo(plan, parity_st)
                                      : cord_plan_parity_group_hull_lo(plan, parity_ingest_stripe_group))
                  : 0;

    auto xor_one_block = [&](int bid) {
      if (!use_merge_subset && cord_plan_data_block_stripe_group(plan, bid) != parity_ingest_stripe_group)
        return;
      const std::string bk =
          cord_collector_block_buf_key(plan.plan_key(), alg2_group, collector_block_id, bid);
      auto it = g_cord_collector_block_delta.find(bk);
      if (it == g_cord_collector_block_delta.end())
        return;
      const std::vector<uint8_t> &delta = it->second;
      const std::vector<std::pair<int, int>> segs = cord_plan_sorted_segs_for_block(plan, bid);
      for (size_t u = 0; u < chunk_len; ++u)
      {
        if (!have_segs || segs.empty())
        {
          const size_t idx = static_cast<size_t>(chunk_off) + u;
          if (delta.size() < idx + 1)
            continue;
          buf_out[u] = static_cast<char>(static_cast<unsigned char>(buf_out[u]) ^
                                          static_cast<unsigned char>(delta[idx]));
          continue;
        }
        const int strip_logical =
            group_hull_lo + static_cast<int>(static_cast<int64_t>(chunk_off) + static_cast<int64_t>(u));
        const int pidx = cord_logical_strip_to_packed_delta_idx(segs, strip_logical);
        if (pidx < 0 || static_cast<size_t>(pidx) >= delta.size())
          continue;
        buf_out[u] = static_cast<char>(static_cast<unsigned char>(buf_out[u]) ^
                                        static_cast<unsigned char>(delta[static_cast<size_t>(pidx)]));
      }
    };

    if (use_merge_subset)
    {
      for (int i = 0; i < parity_st.parity_merge_data_block_ids_size(); ++i)
        xor_one_block(parity_st.parity_merge_data_block_ids(i));
    }
    else
    {
      for (int bid = 0; bid < kblk; ++bid)
        xor_one_block(bid);
    }
    return true;
  }

  static std::shared_ptr<const proxy_proto::CordTransferPlan> cord_lookup_registered_plan(const std::string &pk)
  {
    std::lock_guard<std::mutex> lk(g_cord_plan_reg_mu);
    auto it = g_cord_plans_by_key.find(pk);
    if (it == g_cord_plans_by_key.end())
      return nullptr;
    return it->second;
  }

  static bool cord_uses_matrix_encode(const proxy_proto::CordTransferPlan &plan)
  {
    if (!plan.has_cord_encode_meta() || plan.cord_encode_meta().parity_slice_size() <= 0)
      return false;
    const int et = plan.cord_encode_meta().encode_type();
    return et == static_cast<int>(Azure_LRC) || et == static_cast<int>(Optimal_Cauchy_LRC);
  }

  /**
   * plan 线程结束时清理 MST、收集器 XOR、矩阵路径缓冲。
   * 注意：不得在此处 erase g_cord_plans_by_key。收集器上的本地步骤往往先跑完，若删掉注册表，
   * 其它集群发来的 cordPlanCollectorIngestDataDelta 仍会到达并依赖 cord_lookup_registered_plan；
   * 注册项保留至本进程内下一次 scheduleCordTransferPlan 覆盖同 plan_key（一般为新传输）。
   */
  static void cord_xfer_cleanup_xfer_plan(const std::string &plan_key)
  {
    {
      std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
      g_cord_mst_stream.erase(plan_key);
      for (auto it = g_cord_collector_xor_acc.begin(); it != g_cord_collector_xor_acc.end();)
      {
        if (it->first.size() >= plan_key.size() && it->first.compare(0, plan_key.size(), plan_key) == 0)
          it = g_cord_collector_xor_acc.erase(it);
        else
          ++it;
      }
      for (auto it = g_cord_collector_block_delta.begin(); it != g_cord_collector_block_delta.end();)
      {
        if (it->first.size() >= plan_key.size() && it->first.compare(0, plan_key.size(), plan_key) == 0)
          it = g_cord_collector_block_delta.erase(it);
        else
          ++it;
      }
      for (auto it = g_cord_collector_parity_coded.begin(); it != g_cord_collector_parity_coded.end();)
      {
        if (it->first.size() >= plan_key.size() && it->first.compare(0, plan_key.size(), plan_key) == 0)
          it = g_cord_collector_parity_coded.erase(it);
        else
          ++it;
      }
    }
    cord_pure_xfer_erase_tracker(plan_key);
  }

  namespace
  {
    inline bool is_azure_like_code(const std::string &code_type)
    {
      return code_type == "AzureLRC" || code_type == "RandomLRC" || code_type == "CordD";
    }
  }

  static std::string cord_plan_wall_ts_ms()
  {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto frac_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t t = clock::to_time_t(now);
    std::tm local_tm{};
    localtime_r(&t, &local_tm);
    char buf[40];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &local_tm);
    std::ostringstream oss;
    oss << buf << '.' << std::setfill('0') << std::setw(3) << frac_ms.count();
    return oss.str();
  }

  static const char *cord_plan_delta_kind_name(proxy_proto::CordDeltaPayloadKind k)
  {
    switch (k)
    {
    case proxy_proto::CORD_DELTA_DATA:
      return "DATA_DELTA";
    case proxy_proto::CORD_DELTA_PARITY:
      return "PARITY_DELTA";
    default:
      return "UNKNOWN_DELTA";
    }
  }

  static const char *cord_transfer_link_kind_name(proxy_proto::CordTransferLinkKind k)
  {
    switch (k)
    {
    case proxy_proto::CORD_TRANSFER_STAR_DATA_TO_CENTER:
      return "STAR_DATA_TO_CENTER";
    case proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_GLOBAL:
      return "STAR_CENTER_TO_GLOBAL";
    case proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_LOCAL:
      return "STAR_CENTER_TO_LOCAL";
    case proxy_proto::CORD_TRANSFER_MST_FORWARD:
      return "MST_FORWARD";
    default:
      return "UNKNOWN";
    }
  }

  static bool cord_lookup_cluster_endpoint(const proxy_proto::CordTransferPlan &plan, int cluster_id,
                                           std::string *out_ip, int *out_port)
  {
    for (int i = 0; i < plan.cluster_endpoints_size(); ++i)
    {
      if (plan.cluster_endpoints(i).cluster_id() == cluster_id)
      {
        *out_ip = plan.cluster_endpoints(i).proxy_ip();
        *out_port = plan.cluster_endpoints(i).proxy_port();
        return true;
      }
    }
    return false;
  }


  /** 对本 proxy 作为 src 的所有步骤，预先创建 peer gRPC channel/stub（与步骤循环内 stub_for_peer_proxy 一致）。 */
  static void cord_pure_xfer_peer_stub_preheat(const proxy_proto::CordTransferPlan &plan, int self_cluster_id,
                                               ProxyImpl *proxy)
  {
    std::unordered_set<std::string> endpoints;
    for (int si = 0; si < plan.steps_size(); ++si)
    {
      const proxy_proto::CordTransferStep &st = plan.steps(si);
      if (st.src_proxy_cluster_id() != self_cluster_id)
        continue;
      std::string dst_ip;
      int dst_port = 0;
      if (!cord_lookup_cluster_endpoint(plan, st.dst_proxy_cluster_id(), &dst_ip, &dst_port))
        continue;
      endpoints.insert(dst_ip + ":" + std::to_string(dst_port));
    }
    for (const auto &ep : endpoints)
      (void)proxy->stub_for_peer_proxy(ep);
  }


  static bool cord_lookup_block_placement(const proxy_proto::CordTransferPlan &plan, int block_id,
                                          std::string *bk, std::string *dip, int *dport)
  {
    for (int i = 0; i < plan.block_placements_size(); ++i)
    {
      if (plan.block_placements(i).block_id() == block_id)
      {
        *bk = plan.block_placements(i).block_key();
        *dip = plan.block_placements(i).datanode_ip();
        *dport = plan.block_placements(i).datanode_port();
        return true;
      }
    }
    return false;
  }

  static bool cord_lookup_delta_blob(const proxy_proto::CordTransferPlan &plan, int cluster_id,
                                     std::string *blob_key, std::string *dip, int *dport)
  {
    for (int i = 0; i < plan.delta_blob_refs_size(); ++i)
    {
      if (plan.delta_blob_refs(i).cluster_id() == cluster_id)
      {
        *blob_key = plan.delta_blob_refs(i).delta_blob_key();
        *dip = plan.delta_blob_refs(i).delta_datanode_ip();
        *dport = plan.delta_blob_refs(i).delta_datanode_port();
        return true;
      }
    }
    return false;
  }

  static bool cord_lookup_cluster_delta_layout(const proxy_proto::CordTransferPlan &plan, int cluster_id,
                                                int data_block_id, uint64_t *base_off, uint64_t *block_delta_len)
  {
    for (int i = 0; i < plan.cluster_delta_layouts_size(); ++i)
    {
      const auto &lay = plan.cluster_delta_layouts(i);
      if (lay.cluster_id() != cluster_id)
        continue;
      for (int j = 0; j < lay.data_block_ids_size(); ++j)
      {
        if (lay.data_block_ids(j) == data_block_id)
        {
          *base_off = lay.delta_base_offset(j);
          *block_delta_len = lay.delta_total_length(j);
          return true;
        }
      }
    }
    return false;
  }

  static uint64_t cord_xor_hint_for_group(const proxy_proto::CordTransferPlan &plan, int group_index)
  {
    for (int i = 0; i < plan.group_xor_hints_size(); ++i)
    {
      if (plan.group_xor_hints(i).group_index() == group_index)
        return plan.group_xor_hints(i).xor_accum_byte_length();
    }
    return 0;
  }

  static bool cord_collector_ingress_ready_locked(const proxy_proto::CordTransferPlan &plan, int group,
                                                  int collector_block_id, int parity_ingest_stripe_group)
  {
    const proxy_proto::CordCollectorIngressExpect *ex = nullptr;
    for (int i = 0; i < plan.cord_collector_expects_size(); ++i)
    {
      if (plan.cord_collector_expects(i).group_index() == group &&
          plan.cord_collector_expects(i).collector_block_id() == collector_block_id)
      {
        ex = &plan.cord_collector_expects(i);
        break;
      }
    }
    if (ex == nullptr)
      return false;
    bool any_required = false;
    for (int i = 0; i < ex->src_data_block_ids_size(); ++i)
    {
      const int sid = ex->src_data_block_ids(i);
      if (parity_ingest_stripe_group >= 0 &&
          cord_plan_data_block_stripe_group(plan, sid) != parity_ingest_stripe_group)
        continue;
      any_required = true;
      const std::string bk = cord_collector_block_buf_key(plan.plan_key(), group, collector_block_id, sid);
      auto it = g_cord_collector_block_delta.find(bk);
      if (it == g_cord_collector_block_delta.end() ||
          it->second.size() < static_cast<size_t>(ex->src_delta_total_bytes(i)))
        return false;
    }
    if (parity_ingest_stripe_group >= 0 && !any_required)
      return false;
    return true;
  }

  static bool cord_matrix_encode_strips(int k, int g_m, int l, ECProject::EncodeType et, int strip_size,
                                        const std::vector<std::vector<char>> &data_strips,
                                        std::vector<std::vector<uint8_t>> *coding_out)
  {
    if (static_cast<int>(data_strips.size()) != k)
      return false;
    std::vector<char *> dptrs(static_cast<size_t>(k));
    std::vector<std::vector<char>> coding(static_cast<size_t>(g_m + l), std::vector<char>(strip_size));
    std::vector<char *> cptrs(static_cast<size_t>(g_m + l));
    for (int i = 0; i < k; ++i)
      dptrs[static_cast<size_t>(i)] = const_cast<char *>(data_strips[static_cast<size_t>(i)].data());
    for (int j = 0; j < g_m + l; ++j)
      cptrs[static_cast<size_t>(j)] = coding[static_cast<size_t>(j)].data();
    if (!encode(k, g_m, l, dptrs.data(), cptrs.data(), strip_size, et))
      return false;
    coding_out->resize(static_cast<size_t>(g_m + l));
    for (int j = 0; j < g_m + l; ++j)
      (*coding_out)[static_cast<size_t>(j)].assign(coding[static_cast<size_t>(j)].begin(),
                                                    coding[static_cast<size_t>(j)].end());
    return true;
  }

  /** 等待收集器上 plan 期望的数据增量（可按 parity_ingest_stripe_group 过滤）到齐。 */
  static bool cord_spin_until_collector_ingress_ready(const proxy_proto::CordTransferPlan &plan, int group,
                                                      int collector_block_id, int parity_ingest_stripe_group)
  {
    int spins = 0;
    while (true)
    {
      {
        std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
        if (cord_collector_ingress_ready_locked(plan, group, collector_block_id, parity_ingest_stripe_group))
          return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      if (++spins > 120000)
      {
        std::cout << "[CoRD-PLAN] collector ingress timeout group=" << group << " col=" << collector_block_id
                  << std::endl;
        return false;
      }
    }
  }

  static bool cord_ensure_collector_parity_coded(const proxy_proto::CordTransferPlan &plan, int group,
                                               int collector_block_id, int parity_ingest_stripe_group)
  {
    const std::string ck =
        cord_collector_parity_cache_key(plan.plan_key(), group, collector_block_id, parity_ingest_stripe_group);
    {
      std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
      if (g_cord_collector_parity_coded.count(ck))
        return true;
    }
    if (!cord_spin_until_collector_ingress_ready(plan, group, collector_block_id, parity_ingest_stripe_group))
      return false;
    std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
    if (g_cord_collector_parity_coded.count(ck))
      return true;
    const auto &meta = plan.cord_encode_meta();
    const int k = meta.k();
    const int ps = meta.parity_slice_size();
    const int po = meta.parity_slice_offset();
    std::vector<std::vector<char>> strips(static_cast<size_t>(k), std::vector<char>(static_cast<size_t>(ps), 0));
    for (int didx = 0; didx < plan.cord_data_strip_descs_size(); ++didx)
    {
      const auto &desc = plan.cord_data_strip_descs(didx);
      const int bid = desc.block_id();
      if (bid < 0 || bid >= k)
        continue;
      if (parity_ingest_stripe_group >= 0 &&
          cord_plan_data_block_stripe_group(plan, bid) != parity_ingest_stripe_group)
        continue;
      const std::string bk = cord_collector_block_buf_key(plan.plan_key(), group, collector_block_id, bid);
      auto it = g_cord_collector_block_delta.find(bk);
      if (it == g_cord_collector_block_delta.end())
        continue;
      const std::vector<uint8_t> &delta = it->second;
      const int so = desc.slice_offset();
      const int slen = desc.slice_len();
      const int lo = std::max(so, po);
      const int hi = std::min(so + slen, po + ps);
      for (int x = lo; x < hi; ++x)
      {
        const size_t di_off = static_cast<size_t>(x - so);
        if (di_off >= delta.size())
          continue;
        strips[static_cast<size_t>(bid)][static_cast<size_t>(x - po)] = static_cast<char>(delta[di_off]);
      }
    }
    std::vector<std::vector<uint8_t>> coded;
    const ECProject::EncodeType et = static_cast<ECProject::EncodeType>(meta.encode_type());
    if (!cord_matrix_encode_strips(k, meta.g_m(), meta.l(), et, ps, strips, &coded))
    {
      std::cout << "[CoRD-PLAN] matrix encode failed" << std::endl;
      return false;
    }
    g_cord_collector_parity_coded[ck] = std::move(coded);
    return true;
  }

  /**
   * 按 plan step 顺序执行；coordinator 按算法二的 scheduled_slot 重排步骤，星型与 MST 可穿插。
   * 收集器扇出须晚于同组 STAR 数据到达由算法二时隙依赖保证。
   * N>1：STAR 数据增量 -> cordPlanCollectorIngestDataDelta；收集器再发 cordPlanApplyParityXorDelta。
   * N=1 MST：全程数据增量 cordPlanMstDataDeltaChunk（校验侧矩阵编码或 XOR）。
   */
  static void cord_transfer_plan_execute_async(const proxy_proto::CordTransferPlan &plan, int self_cluster_id,
                                               ProxyImpl *proxy, const std::string &proxy_tag)
  {
      // 打开日志文件: /tmp/cord_transfer_<plan_key>.log
      const std::string log_path = "/tmp/cord_transfer_" + plan.plan_key() + ".log";
      std::ofstream log_ofs(log_path, std::ios::out | std::ios::app);
      const auto wall_now_ns = []() -> int64_t {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
      };
      const auto wall_ts_ms_str = [&]() -> std::string {
        return std::to_string(wall_now_ns() / 1000000LL);
      };

      const auto plan_log = [&](const std::string &msg) {
        log_ofs << "[" << wall_ts_ms_str() << "][" << proxy_tag << "] " << msg << std::endl;
      };

      // 也输出到 stdout 方便实时观察
      const auto plan_log_both = [&](const std::string &msg) {
        const std::string line = "[" + wall_ts_ms_str() + "][" + proxy_tag + "] " + msg;
        log_ofs << line << std::endl;
        std::cout << "[CoRD-XFER] " << line << std::endl;
      };

      plan_log_both("══════ CordTransferPlan EXECUTION START ══════");
      plan_log_both("stripe_id=" + std::to_string(plan.stripe_id()) +
                    " plan_key=" + plan.plan_key() +
                    " total_rounds=" + std::to_string(plan.total_rounds()) +
                    " steps=" + std::to_string(plan.steps_size()) +
                    " self_cluster=c" + std::to_string(self_cluster_id) +
                    " log_file=" + log_path);
      if (plan.steps_size() <= 0)
      {
        plan_log_both("plan has 0 steps, nothing to do");
        log_ofs.close();
        return;
      }

      // 打印完整 plan 概览
      {
        std::ostringstream os;
        os << "Plan overview: rounds=" << plan.total_rounds()
           << " slot_unit=" << plan.slot_unit_bytes() << "B k=" << plan.k_datablock()
           << " encode=" << (cord_uses_matrix_encode(plan) ? "matrix" : "xor");
        plan_log(os.str());
        for (int si = 0; si < plan.steps_size(); ++si) {
          const auto &s = plan.steps(si);
          std::ostringstream ss;
          ss << "  step[" << si << "] slot=" << s.scheduled_slot()
             << " c" << s.src_proxy_cluster_id() << "→c" << s.dst_proxy_cluster_id()
             << " blk" << s.src_block_id() << "→blk" << s.dst_block_id()
             << " chunk=[" << s.chunk_byte_offset() << "+" << s.chunk_byte_length() << "B]"
             << " link=" << cord_transfer_link_kind_name(s.link_kind())
             << " delta=" << cord_plan_delta_kind_name(s.delta_payload_kind())
             << " grp=" << s.group_index();
          if (s.has_mst_origin_data_block_id())
            ss << " mst_origin=" << s.mst_origin_data_block_id();
          if (s.parity_merge_data_block_ids_size() > 0) {
            ss << " merge_src=[";
            for (int mi = 0; mi < s.parity_merge_data_block_ids_size(); ++mi) {
              if (mi > 0) ss << ",";
              ss << s.parity_merge_data_block_ids(mi);
            }
            ss << "]";
          }
          plan_log(ss.str());
        }
      }
      cord_pure_xfer_peer_stub_preheat(plan, self_cluster_id, proxy);
      cord_pure_xfer_note_loop_start(plan.plan_key());
      const auto wall_t0 = std::chrono::steady_clock::now();

      int executed_steps = 0;
      int skipped_steps = 0;
      int failed_steps = 0;

      const int k = plan.k_datablock();
      for (int si = 0; si < plan.steps_size(); ++si)
      {
        const auto t_step0 = std::chrono::steady_clock::now();
        const proxy_proto::CordTransferStep &st = plan.steps(si);
        if (st.src_proxy_cluster_id() != self_cluster_id)
          continue;

        std::string dst_ip;
        int dst_port = 0;
        if (!cord_lookup_cluster_endpoint(plan, st.dst_proxy_cluster_id(), &dst_ip, &dst_port))
        {
          plan_log("abort_step missing_cluster_endpoint dst_cluster_id=" +
                   std::to_string(st.dst_proxy_cluster_id()) + " step_index=" + std::to_string(st.step_index()));
          continue;
        }
        const std::string dst_channel = dst_ip + ":" + std::to_string(dst_port);

        const size_t chunk_len = static_cast<size_t>(st.chunk_byte_length());
        if (chunk_len == 0u)
        {
          plan_log(std::string("SKIP zero_chunk step_index=") + std::to_string(st.step_index()) +
                   " scheduled_slot=" + std::to_string(st.scheduled_slot()) + " link=" +
                   cord_transfer_link_kind_name(st.link_kind()) + " payload=" +
                   cord_plan_delta_kind_name(st.delta_payload_kind()));
          skipped_steps++;
          continue;
        }

        // ---------- N>1：数据增量 -> 收集器 ----------
        if (st.link_kind() == proxy_proto::CORD_TRANSFER_STAR_DATA_TO_CENTER &&
            st.delta_payload_kind() == proxy_proto::CORD_DELTA_DATA)
        {
          std::string blob_key, dn_ip;
          int dn_port = 0;
          if (!cord_lookup_delta_blob(plan, self_cluster_id, &blob_key, &dn_ip, &dn_port))
          {
            plan_log_both("FAIL STAR_DATA_TO_CENTER no_delta_blob step=" + std::to_string(st.step_index()) +
                         " cluster=c" + std::to_string(self_cluster_id));
            failed_steps++;
            continue;
          }
          uint64_t base_off = 0, blk_tot = 0;
          if (!cord_lookup_cluster_delta_layout(plan, self_cluster_id, st.src_block_id(), &base_off, &blk_tot))
          {
            plan_log_both("FAIL STAR_DATA_TO_CENTER no_cluster_delta_layout step=" + std::to_string(st.step_index()) +
                         " cluster=c" + std::to_string(self_cluster_id) + " data_blk=" + std::to_string(st.src_block_id()));
            failed_steps++;
            continue;
          }
          const uint64_t abs_off = base_off + st.chunk_byte_offset();

          // 1) 从本地 datanode 读 delta blob
          const auto t_read0 = std::chrono::steady_clock::now();
          std::vector<char> buf(chunk_len);
          if (!proxy->CordRangeReadFromDatanode(blob_key, 0, static_cast<int>(abs_off), buf.data(), chunk_len,
                                                dn_ip.c_str(), dn_port))
          {
            plan_log_both("FAIL STAR_DATA_TO_CENTER datanode_read_failed step=" + std::to_string(st.step_index()) +
                         " abs_off=" + std::to_string(abs_off) + " bytes=" + std::to_string(chunk_len) +
                         " datanode=" + dn_ip + ":" + std::to_string(dn_port));
            failed_steps++;
            continue;
          }
          const auto t_read1 = std::chrono::steady_clock::now();
          const double read_ms = std::chrono::duration<double, std::milli>(t_read1 - t_read0).count();

          // 2) gRPC 发送到 collector proxy
          const auto t_grpc0 = std::chrono::steady_clock::now();
          proxy_proto::CordPlanCollectorIngestReq req;
          req.set_plan_key(plan.plan_key());
          req.set_group_index(st.group_index());
          req.set_collector_block_id(st.dst_block_id());
          req.set_chunk_byte_offset(st.chunk_byte_offset());
          req.set_chunk_payload(buf.data(), chunk_len);
          req.set_xor_accum_byte_length(cord_xor_hint_for_group(plan, st.group_index()));
          req.set_src_data_block_id(st.src_block_id());
          proxy_proto::proxyService::Stub *stub = proxy->stub_for_peer_proxy(dst_channel);
          proxy_proto::SetReply rep;
          grpc::ClientContext cctx;
          grpc::Status s = stub->cordPlanCollectorIngestDataDelta(&cctx, req, &rep);
          const auto t_grpc1 = std::chrono::steady_clock::now();
          const double grpc_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_grpc0).count();
          const bool ok = s.ok() && rep.ifcommit();
          const double step_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_step0).count();
          {
            std::ostringstream ob;
            ob << (ok ? "OK" : "FAIL")
               << " DATA_TO_CENTER step=" << st.step_index()
               << " slot=" << st.scheduled_slot()
               << " ΔD blk" << st.src_block_id() << "(c" << st.src_proxy_cluster_id()
               << ") → collector_blk" << st.dst_block_id() << "(c" << st.dst_proxy_cluster_id() << ")"
               << " chunk=[" << st.chunk_byte_offset() << "+" << chunk_len << "B]"
               << " dn_read=" << read_ms << "ms"
               << " grpc=" << grpc_ms << "ms"
               << " step_total=" << step_ms << "ms"
               << " grp=" << st.group_index()
               << " dst=" << dst_channel;
            if (!ok) ob << " grpc_err=" << s.error_message();
            if (ok) plan_log(ob.str()); else plan_log_both(ob.str());
            if (!ok) failed_steps++;
            else executed_steps++;
          }
          continue;
        }

        // ---------- N>1：收集器扇出校验增量（矩阵编码 / 退化为 XOR 缓冲） ----------
        if (st.delta_payload_kind() == proxy_proto::CORD_DELTA_PARITY &&
            (st.link_kind() == proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_GLOBAL ||
             st.link_kind() == proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_LOCAL))
        {
          const auto t_compute0 = std::chrono::steady_clock::now();
          std::vector<char> buf(chunk_len);
          bool filled = false;
          std::string parity_compute_src;
          const int parity_payload_abs_lo = cord_plan_parity_payload_abs_lo(plan, st);
          int32_t non_matrix_slice_base = static_cast<int32_t>(
              static_cast<int64_t>(parity_payload_abs_lo) + static_cast<int64_t>(st.chunk_byte_offset()));
          const int parity_ingest =
              st.has_parity_ingest_stripe_group() ? st.parity_ingest_stripe_group() : -1;
          if (cord_uses_matrix_encode(plan))
          {
            if (!cord_ensure_collector_parity_coded(plan, st.group_index(), st.src_block_id(), parity_ingest))
            {
              plan_log_both("FAIL PARITY_FANOUT cord_ensure_collector_parity_coded_failed step=" +
                           std::to_string(st.step_index()) + " collector_blk=" + std::to_string(st.src_block_id()));
              failed_steps++;
              continue;
            }
            const int row = st.dst_block_id() - plan.k_datablock();
            const auto &meta = plan.cord_encode_meta();
            if (row < 0 || row >= meta.g_m() + meta.l())
            {
              plan_log_both("FAIL PARITY_FANOUT bad_row dst_blk=" + std::to_string(st.dst_block_id()) +
                           " row=" + std::to_string(row) + " step=" + std::to_string(st.step_index()));
              failed_steps++;
              continue;
            }
            const std::string pck =
                cord_collector_parity_cache_key(plan.plan_key(), st.group_index(), st.src_block_id(), parity_ingest);
            std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
            auto pit = g_cord_collector_parity_coded.find(pck);
            if (pit == g_cord_collector_parity_coded.end() ||
                static_cast<int>(pit->second.size()) <= row ||
                pit->second[static_cast<size_t>(row)].size() <
                    static_cast<size_t>(st.chunk_byte_offset()) + chunk_len)
            {
              plan_log_both("FAIL PARITY_FANOUT coded_cache_short row=" + std::to_string(row) +
                           " step=" + std::to_string(st.step_index()));
              failed_steps++;
              continue;
            }
            std::memcpy(buf.data(), pit->second[static_cast<size_t>(row)].data() + st.chunk_byte_offset(), chunk_len);
            filled = true;
            parity_compute_src = "matrix_encode_row";
          }
          if (!filled)
          {
            if (st.has_parity_ingest_stripe_group())
            {
              if (!cord_uses_matrix_encode(plan))
              {
                if (!cord_spin_until_collector_ingress_ready(plan, st.group_index(), st.src_block_id(),
                                                             st.parity_ingest_stripe_group()))
                {
                  plan_log_both("FAIL PARITY_FANOUT filtered_xor ingress_timeout collector_blk=" +
                               std::to_string(st.src_block_id()) + " step=" + std::to_string(st.step_index()));
                  failed_steps++;
                  continue;
                }
              }
              cord_filtered_xor_parity_chunk(plan, st.group_index(), st.src_block_id(),
                                             st.parity_ingest_stripe_group(), st, st.chunk_byte_offset(), chunk_len,
                                             buf.data());
              filled = true;
              parity_compute_src = "filtered_xor";
            }
            else
            {
              if (!cord_spin_until_collector_ingress_ready(plan, st.group_index(), st.src_block_id(), -1))
              {
                plan_log_both("FAIL PARITY_FANOUT collector_xor_acc ingress_timeout collector_blk=" +
                             std::to_string(st.src_block_id()) + " step=" + std::to_string(st.step_index()));
                failed_steps++;
                continue;
              }
              const uint64_t acc_off =
                  static_cast<uint64_t>(parity_payload_abs_lo) + static_cast<uint64_t>(st.chunk_byte_offset());
              std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
              const std::string acc_key =
                  cord_collector_acc_key(plan.plan_key(), st.group_index(), st.src_block_id());
              auto it = g_cord_collector_xor_acc.find(acc_key);
              if (it == g_cord_collector_xor_acc.end() || it->second.size() < static_cast<size_t>(acc_off) + chunk_len)
              {
                plan_log_both("FAIL PARITY_FANOUT collector_xor_acc_missing key=" + acc_key +
                             " step=" + std::to_string(st.step_index()));
                failed_steps++;
                continue;
              }
              std::memcpy(buf.data(), it->second.data() + static_cast<size_t>(acc_off), chunk_len);
              parity_compute_src = "collector_xor_acc";
            }
          }
          std::string pbk, pip;
          int pp = 0;
          if (!cord_lookup_block_placement(plan, st.dst_block_id(), &pbk, &pip, &pp))
          {
            plan_log_both("FAIL PARITY_FANOUT block_placement_missing dst_blk=" + std::to_string(st.dst_block_id()) +
                         " step=" + std::to_string(st.step_index()));
            failed_steps++;
            continue;
          }
          const auto t_compute1 = std::chrono::steady_clock::now();
          const double compute_ms = std::chrono::duration<double, std::milli>(t_compute1 - t_compute0).count();

          const int32_t slice_base =
              cord_uses_matrix_encode(plan)
                  ? static_cast<int32_t>(plan.cord_encode_meta().parity_slice_offset() + st.chunk_byte_offset())
                  : non_matrix_slice_base;
          const auto nz = cord_parity_delta_nonzero_span(buf.data(), chunk_len);
          if (nz.second == 0)
          {
            plan_log("SKIP zero_delta step=" + std::to_string(st.step_index())
                     + " slot=" + std::to_string(st.scheduled_slot()) + " link="
                     + cord_transfer_link_kind_name(st.link_kind()) + " payload="
                     + cord_plan_delta_kind_name(st.delta_payload_kind()) + " collector_blk=" + std::to_string(st.src_block_id())
                     + " dst_blk=" + std::to_string(st.dst_block_id())
                     + " compute_src=" + parity_compute_src + " compute=" + std::to_string(compute_ms) + "ms");
            skipped_steps++;
            continue;
          }
          const int32_t slice_off = slice_base + static_cast<int32_t>(nz.first);
          const int32_t send_len = static_cast<int32_t>(nz.second);
          const auto t_grpc0 = std::chrono::steady_clock::now();
          proxy_proto::CordPlanApplyParityXorReq req;
          req.set_plan_key(plan.plan_key());
          req.set_dst_block_id(st.dst_block_id());
          req.set_block_key(pbk);
          req.set_datanode_ip(pip);
          req.set_datanode_port(pp);
          req.set_parity_slice_offset(slice_off);
          req.set_parity_slice_length(send_len);
          req.set_parity_delta_payload(buf.data() + nz.first, static_cast<size_t>(send_len));
          proxy_proto::proxyService::Stub *stub = proxy->stub_for_peer_proxy(dst_channel);
          proxy_proto::SetReply rep;
          grpc::ClientContext cctx;
          grpc::Status s = stub->cordPlanApplyParityXorDelta(&cctx, req, &rep);
          const auto t_grpc1 = std::chrono::steady_clock::now();
          const double grpc_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_grpc0).count();
          const bool ok = s.ok() && rep.ifcommit();
          const double step_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_step0).count();
          {
            const char *tag = (st.link_kind() == proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_GLOBAL)
                                  ? "CTR_TO_GLOBAL" : "CTR_TO_LOCAL";
            std::ostringstream ob;
            ob << (ok ? "OK" : "FAIL")
               << " " << tag << " step=" << st.step_index()
               << " slot=" << st.scheduled_slot()
               << " ΔP collector_blk" << st.src_block_id() << "(c" << st.src_proxy_cluster_id()
               << ") → " << (st.link_kind() == proxy_proto::CORD_TRANSFER_STAR_CENTER_TO_GLOBAL ? "global_blk" : "local_blk")
               << st.dst_block_id() << "(c" << st.dst_proxy_cluster_id() << ")"
               << " chunk=[" << st.chunk_byte_offset() << "+" << chunk_len
               << "B→wire[" << slice_off << "+" << send_len << "B]]"
               << " compute=" << compute_ms << "ms(" << parity_compute_src << ")"
               << " grpc=" << grpc_ms << "ms"
               << " step_total=" << step_ms << "ms"
               << " grp=" << st.group_index()
               << " dst=" << dst_channel;
            if (!ok) ob << " grpc_err=" << s.error_message();
            if (ok) plan_log(ob.str()); else plan_log_both(ob.str());
            if (!ok) failed_steps++;
            else executed_steps++;
          }
          continue;
        }

        // ---------- N=1：MST 上全程传输数据增量 ----------
        if (st.link_kind() == proxy_proto::CORD_TRANSFER_MST_FORWARD)
        {
          std::vector<char> buf(chunk_len);
          std::string mst_buf_src;
          double read_ms = 0.0;
          if (st.src_block_id() < k)
          {
            std::string blob_key, dn_ip;
            int dn_port = 0;
            if (!cord_lookup_delta_blob(plan, self_cluster_id, &blob_key, &dn_ip, &dn_port))
            {
              plan_log_both("FAIL MST_FORWARD no_delta_blob step=" + std::to_string(st.step_index()) +
                           " cluster=c" + std::to_string(self_cluster_id));
              failed_steps++;
              continue;
            }
            uint64_t base_off = 0, blk_tot = 0;
            if (!cord_lookup_cluster_delta_layout(plan, self_cluster_id, st.src_block_id(), &base_off, &blk_tot))
            {
              plan_log_both("FAIL MST_FORWARD no_delta_layout step=" + std::to_string(st.step_index()));
              failed_steps++;
              continue;
            }
            const auto t_read0 = std::chrono::steady_clock::now();
            const uint64_t abs_off = base_off + st.chunk_byte_offset();
            if (!proxy->CordRangeReadFromDatanode(blob_key, 0, static_cast<int>(abs_off), buf.data(), chunk_len,
                                                  dn_ip.c_str(), dn_port))
            {
              plan_log_both("FAIL MST_FORWARD datanode_read_failed step=" + std::to_string(st.step_index()));
              failed_steps++;
              continue;
            }
            const auto t_read1 = std::chrono::steady_clock::now();
            read_ms = std::chrono::duration<double, std::milli>(t_read1 - t_read0).count();
            mst_buf_src = "dn_blob@" + dn_ip + ":" + std::to_string(dn_port);
          }
          else
          {
            std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
            auto it = g_cord_mst_stream.find(plan.plan_key());
            if (it == g_cord_mst_stream.end() ||
                it->second.size() < static_cast<size_t>(st.chunk_byte_offset()) + chunk_len)
            {
              plan_log_both("FAIL MST_FORWARD relay_buffer_missing step=" + std::to_string(st.step_index()));
              failed_steps++;
              continue;
            }
            std::memcpy(buf.data(), it->second.data() + static_cast<size_t>(st.chunk_byte_offset()), chunk_len);
            mst_buf_src = "relay_buffer";
          }
          std::string pbk, pip;
          int pp = 0;
          if (!cord_lookup_block_placement(plan, st.dst_block_id(), &pbk, &pip, &pp))
          {
            plan_log_both("FAIL MST_FORWARD block_placement_missing dst_blk=" + std::to_string(st.dst_block_id()) +
                         " step=" + std::to_string(st.step_index()));
            failed_steps++;
            continue;
          }
          const auto t_grpc0 = std::chrono::steady_clock::now();
          proxy_proto::CordPlanMstDataDeltaReq req;
          req.set_plan_key(plan.plan_key());
          req.set_dst_proxy_cluster_id(st.dst_proxy_cluster_id());
          req.set_dst_block_id(st.dst_block_id());
          req.set_k_datablock(k);
          req.set_chunk_byte_offset(st.chunk_byte_offset());
          req.set_chunk_payload(buf.data(), chunk_len);
          req.set_total_expected_bytes(st.payload_bytes());
          req.set_src_data_block_id(st.has_mst_origin_data_block_id() ? st.mst_origin_data_block_id()
                                                                      : st.src_block_id());
          req.set_parity_block_key(pbk);
          req.set_parity_datanode_ip(pip);
          req.set_parity_datanode_port(pp);
          proxy_proto::proxyService::Stub *stub = proxy->stub_for_peer_proxy(dst_channel);
          proxy_proto::SetReply rep;
          grpc::ClientContext cctx;
          grpc::Status s = stub->cordPlanMstDataDeltaChunk(&cctx, req, &rep);
          const auto t_grpc1 = std::chrono::steady_clock::now();
          const double grpc_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_grpc0).count();
          const bool ok = s.ok() && rep.ifcommit();
          const double step_ms = std::chrono::duration<double, std::milli>(t_grpc1 - t_step0).count();
          {
            const int origin_blk = st.has_mst_origin_data_block_id() ? st.mst_origin_data_block_id() : st.src_block_id();
            std::ostringstream ob;
            ob << (ok ? "OK" : "FAIL")
               << " MST_FORWARD step=" << st.step_index()
               << " slot=" << st.scheduled_slot()
               << " ΔD blk" << st.src_block_id() << "(c" << st.src_proxy_cluster_id()
               << ") → blk" << st.dst_block_id() << "(c" << st.dst_proxy_cluster_id() << ")"
               << " chunk=[" << st.chunk_byte_offset() << "+" << chunk_len << "B]"
               << " origin_data_blk=" << origin_blk
               << " buf_src=" << mst_buf_src;
            if (st.src_block_id() < k) ob << " dn_read=" << read_ms << "ms";
            ob << " grpc=" << grpc_ms << "ms"
               << " step_total=" << step_ms << "ms"
               << " dst=" << dst_channel;
            if (!ok) ob << " grpc_err=" << s.error_message();
            if (ok) plan_log(ob.str()); else plan_log_both(ob.str());
            if (!ok) failed_steps++;
            else executed_steps++;
          }
          continue;
        }

        plan_log_both("SKIP unhandled_link step=" + std::to_string(st.step_index()) + " link=" +
                     cord_transfer_link_kind_name(st.link_kind()) + " payload=" +
                     cord_plan_delta_kind_name(st.delta_payload_kind()));
        skipped_steps++;
      }

      const auto sender_loop_done = std::chrono::steady_clock::now();
      const auto sender_wall_done = std::chrono::system_clock::now();
      cord_pure_xfer_log_after_sender_loop(plan.plan_key(), proxy_tag, sender_loop_done, sender_wall_done);
      cord_xfer_cleanup_xfer_plan(plan.plan_key());
      const auto wall_t1 = std::chrono::steady_clock::now();
      const double wall_sec = std::chrono::duration<double>(wall_t1 - wall_t0).count();
      plan_log_both("══════ CordTransferPlan EXECUTION DONE ══════");
      plan_log_both("plan_key=" + plan.plan_key()
                    + " wall_sec=" + std::to_string(wall_sec)
                    + " executed=" + std::to_string(executed_steps)
                    + " skipped=" + std::to_string(skipped_steps)
                    + " failed=" + std::to_string(failed_steps)
                    + " log_file=" + log_path);
      log_ofs.close();
  }

  bool ProxyImpl::init_coordinator()
  {
    m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(grpc::CreateChannel(m_coordinator_address, grpc::InsecureChannelCredentials()));
    // coordinator_proto::RequestToCoordinator req;
    // coordinator_proto::ReplyFromCoordinator rep;
    // grpc::ClientContext context;
    // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
    // req.set_name(proxy_info);
    // grpc::Status status;
    // status = m_coordinator_ptr->checkalive(&context, req, &rep);
    // if (status.ok())
    // {
    //   std::cout << "[Coordinator Check] ok from " << m_coordinator_address << std::endl;
    // }
    // else
    // {
    //   std::cout << "[Coordinator Check] failed to connect " << m_coordinator_address << std::endl;
    // }
    return true;
  }

  proxy_proto::proxyService::Stub *ProxyImpl::stub_for_peer_proxy(const std::string &endpoint)
  {
    std::lock_guard<std::mutex> lk(m_peer_proxy_stub_mu);
    auto it = m_peer_proxy_stub_pool.find(endpoint);
    if (it != m_peer_proxy_stub_pool.end())
      return it->second->stub.get();
    auto ent = std::make_unique<PeerProxyGrpcEntry>();
    ent->channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    ent->stub = proxy_proto::proxyService::NewStub(ent->channel);
    proxy_proto::proxyService::Stub *s = ent->stub.get();
    m_peer_proxy_stub_pool.emplace(endpoint, std::move(ent));
    return s;
  }

  bool ProxyImpl::init_datanodes(std::string m_datanodeinfo_path)
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_datanodeinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *cluster = root->FirstChildElement(); cluster != nullptr; cluster = cluster->NextSiblingElement())
    {
      std::string cluster_id(cluster->Attribute("id"));
      std::string proxy(cluster->Attribute("proxy"));
      if (proxy == proxy_ip_port)
      {
        m_self_cluster_id = std::stoi(cluster_id);
      }
      for (tinyxml2::XMLElement *node = cluster->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(node_uri, grpc::InsecureChannelCredentials()));
        // datanode_proto::CheckaliveCMD cmd;
        // datanode_proto::RequestResult result;
        // grpc::ClientContext context;
        // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
        // cmd.set_name(proxy_info);
        // grpc::Status status;
        // status = _stub->checkalive(&context, cmd, &result);
        // if (status.ok())
        // {
        //   // std::cout << "[Datanode Check] ok from " << node_uri << std::endl;
        // }
        // else
        // {
        //   std::cout << "[Datanode Check] failed to connect " << node_uri << std::endl;
        // }
        m_datanode_ptrs.insert(std::make_pair(node_uri, std::move(_stub)));
      }
    }
    return true;
  }

  grpc::Status ProxyImpl::checkalive(grpc::ServerContext *context,
                                     const proxy_proto::CheckaliveCMD *request,
                                     proxy_proto::RequestResult *response)
  {

    std::cout << "[Proxy] checkalive" << request->name() << std::endl;
    response->set_message(false);
    init_coordinator();
    return grpc::Status::OK;
  }

  bool ProxyImpl::MergeParityOnDatanode(const char *block_key, int block_id, const char *ip, int port, const std::string &append_mode)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo merge_parity_info;
      datanode_proto::RequestResult result;
      merge_parity_info.set_block_key(std::string(block_key));
      merge_parity_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      // REP_MODE; UNILRC_MODE; CACHED_MODE
      if (append_mode == "UNILRC_MODE" || append_mode == "CACHED_MODE")
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleMergeParity(&context, merge_parity_info, &result);
      }
      /*else if (append_mode == "REP_MODE")
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleMergeParityWithRep(&context, merge_parity_info, &result);
      }*/
      else
      {
        throw std::runtime_error("Invalid append mode: " + append_mode);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  // slice_offset is the physical offset of the data block
  bool ProxyImpl::AppendToDatanode(const char *block_key, int block_id, size_t slice_size, const char *slice_buf, int slice_offset, const char *ip, int port, bool is_serialized)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::AppendInfo append_info;
      datanode_proto::RequestResult result;
      append_info.set_block_key(std::string(block_key));
      append_info.set_block_id(block_id);
      append_info.set_append_size(slice_size);
      append_info.set_append_offset(slice_offset);
      append_info.set_is_serialized(is_serialized);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleAppend(&context, append_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << " slice_size: " << slice_size << " slice_offset: " << slice_offset << " is_serialized: " << is_serialized << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << " slice_size: " << slice_size << " slice_offset: " << slice_offset << " is_serialized: " << is_serialized << std::endl;
        exit(-1);
      }
      asio::write(socket, asio::buffer(slice_buf, slice_size), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Append139]"
                  << "Append to " << block_key << " with length of " << slice_size << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::RecoveryToDatanode(const char *block_key, int block_id, const char *buf, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo recovery_info;
      datanode_proto::RequestResult result;
      recovery_info.set_block_key(std::string(block_key));
      recovery_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::thread notify_datanode_thread([this, &context, &recovery_info, &result, &node_ip_port, &block_key, &block_id]()
      {
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecovery(&context, recovery_info, &result);
        if (!stat.ok())
        {
          std::cout << "[RecoveryToDatanode] notify datanode failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
          exit(-1);
        }
      });
      //grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecovery(&context, recovery_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error)
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << std::endl;
      }
      else
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
        exit(-1);
      }
      asio::write(socket, asio::buffer(buf, m_sys_config->BlockSize), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      notify_datanode_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::RecoveryToDatanodeBreakdown(const char *block_key, int block_id, const char *buf, const char *ip, int port, double *network_time, double *disk_io_time)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::MergeParityInfo recovery_info;
      datanode_proto::RequestResult result;
      recovery_info.set_block_key(std::string(block_key));
      recovery_info.set_block_id(block_id);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::chrono::high_resolution_clock::time_point grpc_notify_time;
      std::thread notify_datanode_thread([this, &context, &recovery_info, &result, &grpc_notify_time, &node_ip_port, &block_key, &block_id]()
      {
        grpc_notify_time = std::chrono::high_resolution_clock::now();
        grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleRecoveryBreakdown(&context, recovery_info, &result);
        if (!stat.ok())
        {
          std::cout << "[RecoveryToDatanode] notify datanode failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
          exit(-1);
        }
      });

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for network
      if (!con_error)
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success! block_key: " << block_key << " block_id: " << block_id << std::endl;
      }
      else
      {
        std::cout << "[RecoveryToDatanode] Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " failed! block_key: " << block_key << " block_id: " << block_id << std::endl;
        exit(-1);
      }
      asio::write(socket, asio::buffer(buf, m_sys_config->BlockSize), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for network
      *network_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - begin).count();
      notify_datanode_thread.join();
      *disk_io_time = result.disk_io_end_time() - result.disk_io_start_time();
      *network_time += result.grpc_start_time() - std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify_time.time_since_epoch()).count();
  
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(m_ip);
      set_info.set_proxy_port(m_port + offset);
      set_info.set_ispull(false);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleSet(&context, set_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + ECProject::DATANODE_PORT_SHIFT << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }
  bool ProxyImpl::GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port, 
    double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double* grpc_notify_time, double *grpc_start_time)
  {
    try
    {

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << " Ready to recieve data from datanode " << std::endl;

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(key);
      get_info.set_block_size(value_length);
      // set proxy ip and port is useless, however, to competitive with the original code, we still need to set it
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      std::chrono::high_resolution_clock::time_point grpc_notify = std::chrono::high_resolution_clock::now();
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGetBreakdown(&context, get_info, &result);
      if (stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << " failed!" << std::endl;
        return false;
      }
      *disk_io_start_time = result.disk_io_start_time();
      *disk_io_end_time = result.disk_io_end_time();
      *grpc_notify_time = std::chrono::duration_cast<std::chrono::duration<double>>(grpc_notify.time_since_epoch()).count();
      *grpc_start_time = result.grpc_start_time();
      

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      std::chrono::high_resolution_clock::time_point begin = std::chrono::high_resolution_clock::now(); // start time for network
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(value, value_length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now(); // end time for network
      *network_start_time = std::chrono::duration_cast<std::chrono::duration<double>>(begin.time_since_epoch()).count();
      *network_end_time = std::chrono::duration_cast<std::chrono::duration<double>>(end.time_since_epoch()).count();
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
      << " Read data from socket with length of " << value_length << std::endl;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }
  bool ProxyImpl::GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      // ready to recieve
      char *buf = new char[value_length];
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Ready to recieve data from datanode " << std::endl;
      }

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(buf, value_length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      memcpy(value, buf, value_length);
      delete buf;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::GetFromDatanode(const std::string &key, char *value, const size_t value_length, const char *ip, const int port)
  {
    try
    {

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << " Ready to recieve data from datanode " << std::endl;

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(key);
      get_info.set_block_size(value_length);
      // set proxy ip and port is useless, however, to competitive with the original code, we still need to set it
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }
      else if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Call datanode to handle get " << key << " failed!" << std::endl;
        return false;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(value, value_length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
      << " Read data from socket with length of " << value_length << std::endl;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::DelInDatanode(std::string key, std::string node_ip_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::DelInfo delinfo;
      datanode_proto::RequestResult response;
      delinfo.set_block_key(key);
      grpc::Status status = m_datanode_ptrs[node_ip_port]->handleDelete(&context, delinfo, &response);
      if (status.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][DEL] delete block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  void ProxyImpl::printAppendStripeDataPlacement(const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement)
  {
    // Print basic info
    std::cout << "=== AppendStripeDataPlacement Info ===" << std::endl;
    std::cout << "Key: " << append_stripe_data_placement->key() << std::endl;
    std::cout << "Stripe ID: " << append_stripe_data_placement->stripe_id() << std::endl;
    std::cout << "Cluster ID: " << append_stripe_data_placement->cluster_id() << std::endl;
    std::cout << "Total Append Size: " << append_stripe_data_placement->append_size() << std::endl;
    std::cout << "Is Merge Parity: " << (append_stripe_data_placement->is_merge_parity() ? "true" : "false") << std::endl;
    std::cout << "Append Mode: " << append_stripe_data_placement->append_mode() << std::endl;
    std::cout << "Is Serialized: " << (append_stripe_data_placement->is_serialized() ? "true" : "false") << std::endl;

    // Print datanode info
    std::cout << "\n=== Datanode Info ===" << std::endl;
    for (int i = 0; i < append_stripe_data_placement->datanodeip_size(); i++)
    {
      std::cout << "Datanode " << i << ":" << std::endl;
      std::cout << "  IP: " << append_stripe_data_placement->datanodeip(i) << std::endl;
      std::cout << "  Port: " << append_stripe_data_placement->datanodeport(i) << std::endl;
    }

    // Print block info
    std::cout << "\n=== Block Info ===" << std::endl;
    for (int i = 0; i < append_stripe_data_placement->blockkeys_size(); i++)
    {
      std::cout << "Block " << i << ":" << std::endl;
      std::cout << "  Key: " << append_stripe_data_placement->blockkeys(i) << std::endl;
      std::cout << "  ID: " << append_stripe_data_placement->blockids(i) << std::endl;
      std::cout << "  Offset: " << append_stripe_data_placement->offsets(i) << std::endl;
      std::cout << "  Size: " << append_stripe_data_placement->sizes(i) << std::endl;
    }
    std::cout << "===================================" << std::endl;
  }


  bool ProxyImpl::CordRangeReadFromDatanode(const std::string &block_key, int block_id, int range_offset, char *out,
                                            size_t length, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::CordRangeRWInfo info;
      datanode_proto::RequestResult result;
      info.set_block_key(block_key);
      info.set_block_id(block_id);
      info.set_range_offset(range_offset);
      info.set_range_length(static_cast<int>(length));
      info.set_proxy_ip(m_ip);
      info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleCordRangeRead(&context, info, &result);
      if (!stat.ok())
        return false;
      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code ec;
      asio::read(socket, asio::buffer(out, length), ec);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !ec;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
  }

  bool ProxyImpl::CordRangeWriteToDatanode(const std::string &block_key, int block_id, int range_offset, const char *data,
                                           size_t length, const char *ip, int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::CordRangeRWInfo info;
      datanode_proto::RequestResult result;
      info.set_block_key(block_key);
      info.set_block_id(block_id);
      info.set_range_offset(range_offset);
      info.set_range_length(static_cast<int>(length));
      info.set_proxy_ip(m_ip);
      info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleCordRangeWrite(&context, info, &result);
      if (!stat.ok())
        return false;
      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code error;
      asio::write(socket, asio::buffer(data, length), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !error;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
  }

  bool ProxyImpl::CordDeltaBlobToDatanode(const std::string &blob_key, const char *data, size_t length, const char *ip,
                                          int port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::CordDeltaBlobInfo info;
      datanode_proto::RequestResult result;
      info.set_blob_key(blob_key);
      info.set_byte_length(static_cast<int>(length));
      info.set_proxy_ip(m_ip);
      info.set_proxy_port(m_port);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleCordDeltaBlob(&context, info, &result);
      if (!stat.ok())
        return false;
      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + ECProject::DATANODE_PORT_SHIFT)}));
      asio::error_code error;
      asio::write(socket, asio::buffer(data, length), error);
      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      return !error;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
      return false;
    }
  }

  grpc::Status ProxyImpl::scheduleCordDataUpdate(
      grpc::ServerContext *context,
      const proxy_proto::CordDataUpdatePlacement *placement,
      proxy_proto::SetReply *response)
  {
    (void)context;
    (void)response;
    const int stripe_id = placement->stripe_id();
    const uint64_t payload_size = placement->update_payload_size();
    const int slice_num = placement->blockkeys_size();
    auto placement_copy = std::make_shared<proxy_proto::CordDataUpdatePlacement>(*placement);

    auto cord_job = [this, stripe_id, payload_size, slice_num, placement_copy]() mutable
    {
      try
      {
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;
        std::vector<char> buf(static_cast<size_t>(payload_size));
        asio::read(socket_data, asio::buffer(buf.data(), static_cast<size_t>(payload_size)), error);
        std::string peer_ep = "unknown";
        try
        {
          auto re = socket_data.remote_endpoint();
          peer_ep = re.address().to_string() + ":" + std::to_string(re.port());
        }
        catch (...)
        {
        }
        // std::cout << "[CoRD-DATA][" << proxy_ip_port << "] recv TCP from client peer=" << peer_ep
        //           << " bytes=" << payload_size << " stripe_id=" << stripe_id << " cluster_id=" << placement_copy->cluster_id()
        //           << " key=" << placement_copy->key() << std::endl;
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        std::vector<size_t> sizes;
        for (int i = 0; i < slice_num; ++i)
          sizes.push_back(static_cast<size_t>(placement_copy->sizes(i)));
        std::vector<char *> slices =
            m_toolbox->splitCharPointer(buf.data(), static_cast<size_t>(payload_size), sizes);

        std::vector<char> delta_concat;
        delta_concat.reserve(static_cast<size_t>(payload_size));
        for (int j = 0; j < slice_num; ++j)
        {
          const size_t slen = sizes[static_cast<size_t>(j)];
          std::vector<char> oldbuf(slen);
          if (!CordRangeReadFromDatanode(placement_copy->blockkeys(j), placement_copy->blockids(j),
                                        static_cast<int>(placement_copy->offsets(j)), oldbuf.data(), slen,
                                        placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j)))
          {
            std::cout << "[CoRD][Proxy] range read failed slice " << j << std::endl;
            return;
          }
          // std::cout << "[CoRD-DATA][" << proxy_ip_port << "] data_blk=" << placement_copy->blockids(j)
          //           << " key=" << placement_copy->blockkeys(j) << " off=" << placement_copy->offsets(j)
          //           << " len=" << slen << " BEFORE_disk_hex=" << cord_dbg_hex_preview(oldbuf.data(), slen)
          //           << " new_slice_hex=" << cord_dbg_hex_preview(slices[static_cast<size_t>(j)], slen) << std::endl;
          for (size_t u = 0; u < slen; ++u)
            delta_concat.push_back(static_cast<char>(oldbuf[u] ^ slices[static_cast<size_t>(j)][u]));
          if (!CordRangeWriteToDatanode(placement_copy->blockkeys(j), placement_copy->blockids(j),
                                        static_cast<int>(placement_copy->offsets(j)), slices[static_cast<size_t>(j)],
                                        slen, placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j)))
          {
            std::cout << "[CoRD][Proxy] range write failed slice " << j << std::endl;
            return;
          }
          std::vector<char> verify_new(slen);
          if (CordRangeReadFromDatanode(placement_copy->blockkeys(j), placement_copy->blockids(j),
                                        static_cast<int>(placement_copy->offsets(j)), verify_new.data(), slen,
                                        placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j)))
          {
            // std::cout << "[CoRD-DATA][" << proxy_ip_port << "] data_blk=" << placement_copy->blockids(j)
            //           << " off=" << placement_copy->offsets(j) << " len=" << slen
            //           << " AFTER_disk_hex=" << cord_dbg_hex_preview(verify_new.data(), slen) << std::endl;
          }
        }
        if (!CordDeltaBlobToDatanode(placement_copy->delta_blob_key(), delta_concat.data(), delta_concat.size(),
                                     placement_copy->delta_datanode_ip().c_str(),
                                     placement_copy->delta_datanode_port()))
        {
          std::cout << "[CoRD][Proxy] delta blob store failed" << std::endl;
          return;
        }

        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext ctx;
        commit_abort_key.set_opp(ECProject::CORD_UPDATE);
        commit_abort_key.set_key(placement_copy->key());
        commit_abort_key.set_stripe_id(stripe_id);
        commit_abort_key.set_ifcommitmetadata(true);
        grpc::Status st = m_coordinator_ptr->reportCommitAbort(&ctx, commit_abort_key, &result);
        if (!st.ok() && IF_DEBUG)
          std::cout << "[CoRD][Proxy] reportCommitAbort failed" << std::endl;
      }
      catch (std::exception &e)
      {
        std::cout << "[CoRD][Proxy] exception: " << e.what() << std::endl;
      }
    };
    std::thread th(cord_job);
    th.detach();
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::scheduleCordTransferPlan(grpc::ServerContext *context,
                                                   const proxy_proto::CordTransferPlan *plan,
                                                   proxy_proto::SetReply *response)
  {
    (void)context;
    // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] RECV grpc scheduleCordTransferPlan peer=" << context->peer()
    //           << " plan_key=" << plan->plan_key() << " stripe_id=" << plan->stripe_id()
    //           << " steps=" << plan->steps_size() << std::endl;
    response->set_ifcommit(false);
    if (plan->plan_key().empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty plan_key");
    auto plan_copy = std::make_shared<proxy_proto::CordTransferPlan>(*plan);
    {
      std::lock_guard<std::mutex> lk(g_cord_plan_reg_mu);
      g_cord_plans_by_key[plan_copy->plan_key()] =
          std::shared_ptr<const proxy_proto::CordTransferPlan>(plan_copy);
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordPlanStartExecution(grpc::ServerContext *context,
                                                 const proxy_proto::CordPlanKeyMsg *request,
                                                 proxy_proto::SetReply *response)
  {
    (void)context;
    response->set_ifcommit(false);
    const std::string &pk = request->plan_key();
    if (pk.empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty plan_key");
    auto plan_ptr = cord_lookup_registered_plan(pk);
    if (!plan_ptr)
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "cord plan_key not registered");
    const int self_cid = m_self_cluster_id;
    const std::string tag = proxy_ip_port;
    std::thread th([plan_ptr, self_cid, tag, p = this]() {
      cord_transfer_plan_execute_async(*plan_ptr, self_cid, p, tag);
    });
    {
      std::lock_guard<std::mutex> lk(g_cord_plan_exec_mu);
      auto it = g_cord_plan_exec_threads.find(pk);
      if (it != g_cord_plan_exec_threads.end() && it->second.joinable())
        it->second.join();
      g_cord_plan_exec_threads[pk] = std::move(th);
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordPlanJoinExecution(grpc::ServerContext *context,
                                                const proxy_proto::CordPlanKeyMsg *request,
                                                proxy_proto::SetReply *response)
  {
    (void)context;
    response->set_ifcommit(false);
    const std::string &pk = request->plan_key();
    if (pk.empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty plan_key");
    std::thread worker;
    {
      std::lock_guard<std::mutex> lk(g_cord_plan_exec_mu);
      auto it = g_cord_plan_exec_threads.find(pk);
      if (it == g_cord_plan_exec_threads.end())
      {
        response->set_ifcommit(true);
        return grpc::Status::OK;
      }
      worker = std::move(it->second);
      g_cord_plan_exec_threads.erase(it);
    }
    if (worker.joinable())
      worker.join();
    {
      std::lock_guard<std::mutex> lk(g_cord_join_xfer_timing_mu);
      auto it = g_cord_join_xfer_timing_by_plan.find(pk);
      if (it != g_cord_join_xfer_timing_by_plan.end())
      {
        const CordJoinXferTimingSample &s = it->second;
        response->set_cord_join_xfer_timing_present(true);
        response->set_cord_join_pure_xfer_sec(s.pure_xfer_sec);
        response->set_cord_join_pure_xfer_start_unix_ms(s.wall_start_unix_ms);
        response->set_cord_join_pure_xfer_end_unix_ms(s.wall_end_unix_ms);
        g_cord_join_xfer_timing_by_plan.erase(it);
      }
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordPlanCollectorIngestDataDelta(
      grpc::ServerContext *context,
      const proxy_proto::CordPlanCollectorIngestReq *request,
      proxy_proto::SetReply *response)
  {
    response->set_ifcommit(false);
    const auto &chunk = request->chunk_payload();
    auto pl = cord_lookup_registered_plan(request->plan_key());
    if (!pl)
    {
      std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] REJECT cordPlanCollectorIngestDataDelta peer="
                << context->peer() << " plan_key=" << request->plan_key() << " grp=" << request->group_index()
                << " collector_blk=" << request->collector_block_id() << " src_data_blk=" << request->src_data_block_id()
                << " chunk_off=" << request->chunk_byte_offset() << " chunk_len=" << chunk.size()
                << " reason=plan_key_not_registered" << std::endl;
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "cord plan_key not registered");
    }
    // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] RECV grpc cordPlanCollectorIngest peer=" << context->peer()
    //           << " plan_key=" << request->plan_key() << " grp=" << request->group_index()
    //           << " collector_blk=" << request->collector_block_id() << " src_data_blk=" << request->src_data_block_id()
    //           << " chunk_off=" << request->chunk_byte_offset() << " chunk_len=" << chunk.size()
    //           << " chunk_hex=" << cord_dbg_hex_preview(chunk.data(), chunk.size()) << std::endl;
    if (cord_uses_matrix_encode(*pl) && request->src_data_block_id() < 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "src_data_block_id required");
    const uint64_t off = request->chunk_byte_offset();
    const uint64_t need = off + static_cast<uint64_t>(chunk.size());
    {
      std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
      if (request->src_data_block_id() >= 0)
      {
        const std::string bkey =
            cord_collector_block_buf_key(request->plan_key(), request->group_index(), request->collector_block_id(),
                                         request->src_data_block_id());
        auto &bd = g_cord_collector_block_delta[bkey];
        if (bd.size() < static_cast<size_t>(need))
          bd.resize(static_cast<size_t>(need), 0);
        std::memcpy(bd.data() + static_cast<size_t>(off), chunk.data(), chunk.size());
      }
      if (!cord_uses_matrix_encode(*pl))
      {
        const std::string key =
            cord_collector_acc_key(request->plan_key(), request->group_index(), request->collector_block_id());
        auto &acc = g_cord_collector_xor_acc[key];
        const uint64_t hint = request->xor_accum_byte_length();
        if (hint > 0u && acc.size() < static_cast<size_t>(hint))
          acc.resize(static_cast<size_t>(hint), 0);
        if (acc.size() < static_cast<size_t>(need))
          acc.resize(static_cast<size_t>(need), 0);
        for (size_t i = 0; i < chunk.size(); ++i)
          acc[static_cast<size_t>(off) + i] ^= static_cast<uint8_t>(chunk[static_cast<int>(i)]);
      }
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordPlanApplyParityXorDelta(
      grpc::ServerContext *context,
      const proxy_proto::CordPlanApplyParityXorReq *request,
      proxy_proto::SetReply *response)
  {
    response->set_ifcommit(false);
    const int psz = request->parity_slice_length();
    if (psz <= 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad parity slice");
    std::vector<char> cur(static_cast<size_t>(psz));
    if (!CordRangeReadFromDatanode(request->block_key(), request->dst_block_id(), request->parity_slice_offset(),
                                   cur.data(), static_cast<size_t>(psz), request->datanode_ip().c_str(),
                                   request->datanode_port()))
      return grpc::Status(grpc::StatusCode::INTERNAL, "read parity failed");
    // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] RECV grpc cordPlanApplyParityXor peer=" << context->peer()
    //           << " plan_key=" << request->plan_key() << " parity_blk=" << request->dst_block_id()
    //           << " off=" << request->parity_slice_offset() << " len=" << psz
    //           << " BEFORE_disk_hex=" << cord_dbg_hex_preview(cur.data(), cur.size())
    //           << " delta_hex=" << cord_dbg_hex_preview(request->parity_delta_payload().data(),
    //                                                    request->parity_delta_payload().size())
    //           << std::endl;
    if (static_cast<int>(request->parity_delta_payload().size()) != psz)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "delta size mismatch");
    for (int u = 0; u < psz; ++u)
      cur[static_cast<size_t>(u)] = static_cast<char>(
          static_cast<unsigned char>(cur[static_cast<size_t>(u)]) ^
          static_cast<unsigned char>(request->parity_delta_payload()[static_cast<int>(u)]));
    cord_pure_xfer_parity_dn_begin(request->plan_key());
    if (!CordRangeWriteToDatanode(request->block_key(), request->dst_block_id(), request->parity_slice_offset(),
                                   cur.data(), static_cast<size_t>(psz), request->datanode_ip().c_str(),
                                   request->datanode_port()))
    {
      cord_pure_xfer_parity_dn_abort(request->plan_key());
      return grpc::Status(grpc::StatusCode::INTERNAL, "write parity failed");
    }
    std::vector<char> verify(static_cast<size_t>(psz));
    if (CordRangeReadFromDatanode(request->block_key(), request->dst_block_id(), request->parity_slice_offset(),
                                  verify.data(), static_cast<size_t>(psz), request->datanode_ip().c_str(),
                                  request->datanode_port()))
    {
      // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] parity_blk=" << request->dst_block_id()
      //           << " off=" << request->parity_slice_offset() << " AFTER_disk_hex="
      //           << cord_dbg_hex_preview(verify.data(), verify.size()) << std::endl;
    }
    cord_pure_xfer_parity_dn_done_verified(request->plan_key());
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordPlanMstDataDeltaChunk(
      grpc::ServerContext *context,
      const proxy_proto::CordPlanMstDataDeltaReq *request,
      proxy_proto::SetReply *response)
  {
    response->set_ifcommit(false);
    const std::string &pk = request->plan_key();
    const uint64_t off = request->chunk_byte_offset();
    const std::string &chunk = request->chunk_payload();
    if (chunk.empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty chunk");

    {
      std::lock_guard<std::mutex> lk(g_cord_xfer_mu);
      auto &stream = g_cord_mst_stream[pk];
      const uint64_t need = off + static_cast<uint64_t>(chunk.size());
      if (stream.size() < static_cast<size_t>(need))
        stream.resize(static_cast<size_t>(need), 0);
      for (size_t i = 0; i < chunk.size(); ++i)
        stream[static_cast<size_t>(off) + i] = static_cast<uint8_t>(chunk[static_cast<int>(i)]);
    }

    if (request->dst_proxy_cluster_id() == m_self_cluster_id &&
        request->dst_block_id() >= request->k_datablock())
    {
      // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] RECV grpc cordPlanMstDataDeltaChunk peer=" << context->peer()
      //           << " plan_key=" << pk << " dst_blk=" << request->dst_block_id() << " src_data_blk="
      //           << request->src_data_block_id() << " chunk_off=" << off << " chunk_len=" << chunk.size()
      //           << " chunk_hex=" << cord_dbg_hex_preview(chunk.data(), chunk.size()) << std::endl;
      bool applied_matrix = false;
      auto pl = cord_lookup_registered_plan(pk);
      if (pl && cord_uses_matrix_encode(*pl) && request->src_data_block_id() >= 0)
      {
        const auto &meta = pl->cord_encode_meta();
        const int kblk = meta.k();
        const int ps = meta.parity_slice_size();
        const int po = meta.parity_slice_offset();
        const int src_bid = request->src_data_block_id();
        const proxy_proto::CordDataStripDesc *sdesc = nullptr;
        for (int di = 0; di < pl->cord_data_strip_descs_size(); ++di)
        {
          if (pl->cord_data_strip_descs(di).block_id() == src_bid)
          {
            sdesc = &pl->cord_data_strip_descs(di);
            break;
          }
        }
        if (sdesc != nullptr && src_bid >= 0 && src_bid < kblk && ps > 0)
        {
          const int so = sdesc->slice_offset();
          std::vector<std::vector<char>> strips(static_cast<size_t>(kblk),
                                                std::vector<char>(static_cast<size_t>(ps), 0));
          for (size_t i = 0; i < chunk.size(); ++i)
          {
            const int x = so + static_cast<int>(off) + static_cast<int>(i);
            if (x >= po && x < po + ps)
              strips[static_cast<size_t>(src_bid)][static_cast<size_t>(x - po)] =
                  chunk[static_cast<int>(i)];
          }
          std::vector<std::vector<uint8_t>> coded;
          const ECProject::EncodeType et = static_cast<ECProject::EncodeType>(meta.encode_type());
          if (!cord_matrix_encode_strips(kblk, meta.g_m(), meta.l(), et, ps, strips, &coded))
            return grpc::Status(grpc::StatusCode::INTERNAL, "mst matrix encode failed");
          const int row = request->dst_block_id() - kblk;
          if (row < 0 || row >= static_cast<int>(coded.size()))
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "mst bad parity row");
          const auto &pdelta = coded[static_cast<size_t>(row)];
          const auto nz =
              cord_parity_delta_nonzero_span(reinterpret_cast<const char *>(pdelta.data()), pdelta.size());
          if (nz.second > 0)
          {
            const int poff = static_cast<int>(nz.first);
            const int plen = static_cast<int>(nz.second);
            std::vector<char> cur(static_cast<size_t>(plen));
            if (!CordRangeReadFromDatanode(request->parity_block_key(), request->dst_block_id(), po + poff, cur.data(),
                                           static_cast<size_t>(plen), request->parity_datanode_ip().c_str(),
                                           request->parity_datanode_port()))
              return grpc::Status(grpc::StatusCode::INTERNAL, "read parity failed (mst mat)");
            // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_matrix parity_blk=" << request->dst_block_id()
            //           << " row=" << row << " strip_off=" << po << " strip_len=" << ps << " sparse_off=" << poff
            //           << " sparse_len=" << plen << " BEFORE_disk_hex=" << cord_dbg_hex_preview(cur.data(), cur.size())
            //           << " coded_delta_hex="
            //           << cord_dbg_hex_preview(pdelta.data() + static_cast<size_t>(poff), static_cast<size_t>(plen))
            //           << std::endl;
            for (int u = 0; u < plen; ++u)
              cur[static_cast<size_t>(u)] = static_cast<char>(
                  static_cast<unsigned char>(cur[static_cast<size_t>(u)]) ^
                  static_cast<unsigned char>(pdelta[static_cast<size_t>(poff + u)]));
            cord_pure_xfer_parity_dn_begin(pk);
            if (!CordRangeWriteToDatanode(request->parity_block_key(), request->dst_block_id(), po + poff, cur.data(),
                                          static_cast<size_t>(plen), request->parity_datanode_ip().c_str(),
                                          request->parity_datanode_port()))
            {
              cord_pure_xfer_parity_dn_abort(pk);
              return grpc::Status(grpc::StatusCode::INTERNAL, "write parity failed (mst mat)");
            }
            {
              std::vector<char> mst_pv(static_cast<size_t>(plen));
              if (CordRangeReadFromDatanode(request->parity_block_key(), request->dst_block_id(), po + poff,
                                            mst_pv.data(), static_cast<size_t>(plen), request->parity_datanode_ip().c_str(),
                                            request->parity_datanode_port()))
              {
                // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_matrix AFTER parity_blk=" << request->dst_block_id()
                //           << " disk_hex=" << cord_dbg_hex_preview(mst_pv.data(), mst_pv.size()) << std::endl;
              }
            }
            cord_pure_xfer_parity_dn_done_verified(pk);
          }
          else
          {
            // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_matrix SKIP all-zero parity delta blk="
            //           << request->dst_block_id() << " row=" << row << std::endl;
          }
          applied_matrix = true;
        }
      }
      if (!applied_matrix)
      {
        const auto nz = cord_parity_delta_nonzero_span(chunk.data(), chunk.size());
        if (nz.second == 0)
        {
          // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_xor SKIP all-zero chunk parity_blk="
          //           << request->dst_block_id() << std::endl;
        }
        else
        {
          const int psz = static_cast<int>(nz.second);
          const int slice_off = static_cast<int>(off) + static_cast<int>(nz.first);
          std::vector<char> cur(static_cast<size_t>(psz));
          if (!CordRangeReadFromDatanode(request->parity_block_key(), request->dst_block_id(), slice_off, cur.data(),
                                         static_cast<size_t>(psz), request->parity_datanode_ip().c_str(),
                                         request->parity_datanode_port()))
            return grpc::Status(grpc::StatusCode::INTERNAL, "read parity failed (mst)");
          // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_xor parity_blk=" << request->dst_block_id()
          //           << " off=" << slice_off << " len=" << psz
          //           << " BEFORE_disk_hex=" << cord_dbg_hex_preview(cur.data(), cur.size())
          //           << " chunk_hex=" << cord_dbg_hex_preview(chunk.data() + nz.first, static_cast<size_t>(psz))
          //           << std::endl;
          for (int u = 0; u < psz; ++u)
            cur[static_cast<size_t>(u)] = static_cast<char>(
                static_cast<unsigned char>(cur[static_cast<size_t>(u)]) ^
                static_cast<unsigned char>(chunk[static_cast<int>(nz.first + static_cast<size_t>(u))]));
          cord_pure_xfer_parity_dn_begin(pk);
          if (!CordRangeWriteToDatanode(request->parity_block_key(), request->dst_block_id(), slice_off, cur.data(),
                                        static_cast<size_t>(psz), request->parity_datanode_ip().c_str(),
                                        request->parity_datanode_port()))
          {
            cord_pure_xfer_parity_dn_abort(pk);
            return grpc::Status(grpc::StatusCode::INTERNAL, "write parity failed (mst)");
          }
          {
            std::vector<char> mst_lv(static_cast<size_t>(psz));
            if (CordRangeReadFromDatanode(request->parity_block_key(), request->dst_block_id(), slice_off, mst_lv.data(),
                                          static_cast<size_t>(psz), request->parity_datanode_ip().c_str(),
                                          request->parity_datanode_port()))
            {
              // std::cout << "[CoRD-PLAN][" << proxy_ip_port << "] MST_xor AFTER parity_blk=" << request->dst_block_id()
              //           << " disk_hex=" << cord_dbg_hex_preview(mst_lv.data(), mst_lv.size()) << std::endl;
            }
          }
          cord_pure_xfer_parity_dn_done_verified(pk);
        }
      }
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::scheduleCordLocalParityApply(
      grpc::ServerContext *context,
      const proxy_proto::CordLocalParityBundle *bundle,
      proxy_proto::SetReply *response)
  {
    (void)response;
    // std::cout << "[CoRD-LP][" << proxy_ip_port << "] RECV grpc scheduleCordLocalParityApply peer=" << context->peer()
    //           << " bundle_key=" << bundle->key() << " items=" << bundle->items_size() << std::endl;
    auto bundle_copy = std::make_shared<proxy_proto::CordLocalParityBundle>(*bundle);
    auto lp_job = [this, bundle_copy]() mutable
    {
      try
      {
        for (int ii = 0; ii < bundle_copy->items_size(); ++ii)
        {
          const auto &it = bundle_copy->items(ii);
          const int psz = it.parity_slice_size();
          if (psz <= 0)
            continue;
          std::vector<char> acc(static_cast<size_t>(psz));
          if (!CordRangeReadFromDatanode(it.local_block_key(), it.local_block_id(), it.parity_slice_offset(),
                                        acc.data(), static_cast<size_t>(psz), it.local_datanode_ip().c_str(),
                                        it.local_datanode_port()))
          {
            std::cout << "[CoRD-LP][Proxy] read LP blk " << it.local_block_id() << " failed" << std::endl;
            continue;
          }
          // std::cout << "[CoRD-LP][" << proxy_ip_port << "] BEFORE_local_parity_apply blk=" << it.local_block_id()
          //           << " off=" << it.parity_slice_offset() << " len=" << psz
          //           << " disk_hex=" << cord_dbg_hex_preview(acc.data(), acc.size()) << " bundle_key="
          //           << bundle_copy->key() << std::endl;
          for (int fi = 0; fi < it.fetches_size(); ++fi)
          {
            const auto &f = it.fetches(fi);
            const size_t rlen = static_cast<size_t>(f.read_len());
            if (rlen == 0)
              continue;
            std::vector<char> chunk(rlen);
            if (!CordRangeReadFromDatanode(f.blob_key(), 0, static_cast<int>(f.blob_offset()), chunk.data(), rlen,
                                          f.datanode_ip().c_str(), f.datanode_port()))
            {
              std::cout << "[CoRD-LP][Proxy] read delta blob " << f.blob_key() << " failed" << std::endl;
              continue;
            }
            const int acc_off = f.acc_offset();
            if (acc_off < 0 || static_cast<size_t>(acc_off) + rlen > acc.size())
            {
              std::cout << "[CoRD-LP][Proxy] acc_offset/len out of range" << std::endl;
              continue;
            }
            for (size_t u = 0; u < rlen; ++u)
              acc[static_cast<size_t>(acc_off) + u] =
                  static_cast<char>(static_cast<unsigned char>(acc[static_cast<size_t>(acc_off) + u]) ^
                                    static_cast<unsigned char>(chunk[u]));
          }
          if (!CordRangeWriteToDatanode(it.local_block_key(), it.local_block_id(), it.parity_slice_offset(),
                                        acc.data(), static_cast<size_t>(psz), it.local_datanode_ip().c_str(),
                                        it.local_datanode_port()))
          {
            std::cout << "[CoRD-LP][Proxy] write LP blk " << it.local_block_id() << " failed" << std::endl;
            continue;
          }
          std::vector<char> lp_verify(static_cast<size_t>(psz));
          if (CordRangeReadFromDatanode(it.local_block_key(), it.local_block_id(), it.parity_slice_offset(),
                                        lp_verify.data(), static_cast<size_t>(psz), it.local_datanode_ip().c_str(),
                                        it.local_datanode_port()))
          {
            // std::cout << "[CoRD-LP][" << proxy_ip_port << "] AFTER_local_parity_write blk=" << it.local_block_id()
            //           << " off=" << it.parity_slice_offset() << " len=" << psz
            //           << " disk_hex=" << cord_dbg_hex_preview(lp_verify.data(), lp_verify.size()) << std::endl;
          }
          if (IF_DEBUG)
            std::cout << "[CoRD-LP][Proxy] LP blk " << it.local_block_id() << " off=" << it.parity_slice_offset()
                      << " len=" << psz << " fetches=" << it.fetches_size() << std::endl;
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "[CoRD-LP][Proxy] exception: " << e.what() << std::endl;
      }
    };
    std::thread th(lp_job);
    th.detach();
    return grpc::Status::OK;
  }



  namespace
  {
    struct CordLpHubSessionState
    {
      int expected_partials = 0;
      int received_partials = 0;
      std::vector<uint8_t> acc;
      proxy_proto::CordLpHubSessionBegin meta;
      std::mutex mu;
      bool finished = false;
    };
    std::mutex g_cord_lp_hub_mu;
    std::unordered_map<std::string, std::shared_ptr<CordLpHubSessionState>> g_cord_lp_hub;

    static void cord_lp_hub_forward_to_lp_proxy(ProxyImpl *proxy, const proxy_proto::CordLpHubSessionBegin &meta,
                                                const std::vector<uint8_t> &delta)
    {
      std::string dst = meta.dest_lp_proxy_ip() + ":" + std::to_string(meta.dest_lp_proxy_port());
      proxy_proto::proxyService::Stub *stub = proxy->stub_for_peer_proxy(dst);
      proxy_proto::CordLpParityApplyDelta req;
      req.set_stripe_id(meta.stripe_id());
      req.set_local_block_id(meta.local_block_id());
      req.set_local_block_key(meta.local_block_key());
      req.set_local_datanode_ip(meta.local_datanode_ip());
      req.set_local_datanode_port(meta.local_datanode_port());
      req.set_parity_slice_offset(meta.parity_slice_offset());
      req.set_parity_slice_size(meta.parity_slice_size());
      req.set_delta_payload(delta.data(), delta.size());
      grpc::ClientContext ctx;
      proxy_proto::SetReply rep;
      grpc::Status st = stub->cordLpApplyParityDelta(&ctx, req, &rep);
      if (!st.ok())
        std::cout << "[CoRD-LP-GH] forward to LP proxy " << dst << " failed: " << st.error_message() << std::endl;
      // else
      //   std::cout << "[CoRD-LP-GH] grpc SEND hub -> LP_proxy=" << dst << " stripe=" << meta.stripe_id()
      //             << " hub_G=" << meta.hub_global_block_id() << " lp_blk=" << meta.local_block_id()
      //             << " delta_bytes=" << delta.size() << " delta_preview=" << cord_dbg_hex_preview(delta.data(), delta.size())
      //             << std::endl;
      (void)dst;
      (void)meta;
      (void)delta;
    }
  } // namespace

  grpc::Status ProxyImpl::cordLpHubSessionBegin(
      grpc::ServerContext *context,
      const proxy_proto::CordLpHubSessionBegin *request,
      proxy_proto::SetReply *response)
  {
    (void)context;
    response->set_ifcommit(false);
    if (request->session_key().empty() || request->expected_partials() <= 0 ||
        request->parity_slice_size() <= 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid CordLpHubSessionBegin");
    std::lock_guard<std::mutex> lk(g_cord_lp_hub_mu);
    if (g_cord_lp_hub.find(request->session_key()) != g_cord_lp_hub.end())
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "duplicate session_key");
    auto st = std::make_shared<CordLpHubSessionState>();
    st->expected_partials = request->expected_partials();
    st->acc.assign(static_cast<size_t>(request->parity_slice_size()), 0);
    st->meta.CopyFrom(*request);
    g_cord_lp_hub[request->session_key()] = st;
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordLpHubPartialPush(
      grpc::ServerContext *context,
      const proxy_proto::CordLpHubPartialPush *request,
      proxy_proto::SetReply *response)
  {
    (void)context;
    response->set_ifcommit(false);
    const std::string &sk = request->session_key();
    std::shared_ptr<CordLpHubSessionState> st;
    {
      std::lock_guard<std::mutex> lk(g_cord_lp_hub_mu);
      auto it = g_cord_lp_hub.find(sk);
      if (it == g_cord_lp_hub.end())
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown session_key");
      st = it->second;
    }
    proxy_proto::CordLpHubSessionBegin meta_copy;
    std::vector<uint8_t> forward_delta;
    bool do_erase = false;
    {
      std::lock_guard<std::mutex> lk(st->mu);
      if (st->finished)
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "session already finished");
      if (static_cast<int>(request->partial_payload().size()) != st->meta.parity_slice_size())
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "partial size mismatch");
      if (st->received_partials >= st->expected_partials)
        return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "too many partials");
      for (size_t u = 0; u < st->acc.size(); ++u)
        st->acc[u] = static_cast<uint8_t>(st->acc[u] ^ static_cast<uint8_t>(request->partial_payload()[u]));
      st->received_partials++;
      if (st->received_partials == st->expected_partials)
      {
        st->finished = true;
        meta_copy.CopyFrom(st->meta);
        forward_delta = st->acc;
        do_erase = true;
      }
    }
    if (do_erase)
    {
      cord_lp_hub_forward_to_lp_proxy(this, meta_copy, forward_delta);
      std::lock_guard<std::mutex> lk(g_cord_lp_hub_mu);
      g_cord_lp_hub.erase(sk);
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordLpComputePartialAndPush(
      grpc::ServerContext *context,
      const proxy_proto::CordLpComputePartialAndPush *request,
      proxy_proto::SetReply *response)
  {
    (void)context;
    response->set_ifcommit(false);
    const int psz = request->parity_slice_size();
    if (psz <= 0 || request->session_key().empty())
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad request");
    std::vector<char> acc(static_cast<size_t>(psz), 0);
    for (int fi = 0; fi < request->fetches_size(); ++fi)
    {
      const auto &f = request->fetches(fi);
      const size_t rlen = static_cast<size_t>(f.read_len());
      if (rlen == 0)
        continue;
      std::vector<char> chunk(rlen);
      const std::string &bk = f.blob_key().empty() ? request->delta_blob_key() : f.blob_key();
      const std::string &dip = f.datanode_ip().empty() ? request->delta_datanode_ip() : f.datanode_ip();
      const int dport = f.datanode_port() != 0 ? f.datanode_port() : request->delta_datanode_port();
      if (!CordRangeReadFromDatanode(bk, 0, static_cast<int>(f.blob_offset()), chunk.data(), rlen,
                                     dip.c_str(), dport))
      {
        std::cout << "[CoRD-LP-GH] read delta blob " << bk << " failed" << std::endl;
        return grpc::Status(grpc::StatusCode::INTERNAL, "delta blob read failed");
      }
      const int acc_off = f.acc_offset();
      if (acc_off < 0 || static_cast<size_t>(acc_off) + rlen > acc.size())
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "acc_offset/len out of range");
      for (size_t u = 0; u < rlen; ++u)
        acc[static_cast<size_t>(acc_off) + u] =
            static_cast<char>(static_cast<unsigned char>(acc[static_cast<size_t>(acc_off) + u]) ^
                              static_cast<unsigned char>(chunk[u]));
    }
    std::string hub_addr = request->hub_proxy_ip() + ":" + std::to_string(request->hub_proxy_port());
    proxy_proto::proxyService::Stub *hub_stub = stub_for_peer_proxy(hub_addr);
    proxy_proto::CordLpHubPartialPush push;
    push.set_session_key(request->session_key());
    push.set_partial_payload(acc.data(), acc.size());
    grpc::ClientContext ctx;
    proxy_proto::SetReply rep;
    grpc::Status st = hub_stub->cordLpHubPartialPush(&ctx, push, &rep);
    if (!st.ok())
      return st;
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::cordLpApplyParityDelta(
      grpc::ServerContext *context,
      const proxy_proto::CordLpParityApplyDelta *request,
      proxy_proto::SetReply *response)
  {
    response->set_ifcommit(false);
    const int psz = request->parity_slice_size();
    if (psz <= 0)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "bad parity slice");
    std::vector<char> cur(static_cast<size_t>(psz));
    if (!CordRangeReadFromDatanode(request->local_block_key(), request->local_block_id(),
                                   request->parity_slice_offset(), cur.data(), static_cast<size_t>(psz),
                                   request->local_datanode_ip().c_str(), request->local_datanode_port()))
      return grpc::Status(grpc::StatusCode::INTERNAL, "read local parity failed");
    // std::cout << "[CoRD-LP-GH][" << proxy_ip_port << "] RECV cordLpApplyParityDelta peer=" << context->peer()
    //           << " stripe=" << request->stripe_id() << " blk=" << request->local_block_id()
    //           << " off=" << request->parity_slice_offset() << " len=" << psz
    //           << " BEFORE_hex=" << cord_dbg_hex_preview(cur.data(), cur.size())
    //           << " delta_hex=" << cord_dbg_hex_preview(request->delta_payload().data(), request->delta_payload().size())
    //           << std::endl;
    if (static_cast<int>(request->delta_payload().size()) != psz)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "delta size mismatch");
    for (int u = 0; u < psz; ++u)
      cur[static_cast<size_t>(u)] =
          static_cast<char>(static_cast<unsigned char>(cur[static_cast<size_t>(u)]) ^
                            static_cast<unsigned char>(request->delta_payload()[u]));
    if (!CordRangeWriteToDatanode(request->local_block_key(), request->local_block_id(),
                                  request->parity_slice_offset(), cur.data(), static_cast<size_t>(psz),
                                  request->local_datanode_ip().c_str(), request->local_datanode_port()))
      return grpc::Status(grpc::StatusCode::INTERNAL, "write local parity failed");
    std::vector<char> ghv(static_cast<size_t>(psz));
    if (CordRangeReadFromDatanode(request->local_block_key(), request->local_block_id(),
                                  request->parity_slice_offset(), ghv.data(), static_cast<size_t>(psz),
                                  request->local_datanode_ip().c_str(), request->local_datanode_port()))
    {
      // std::cout << "[CoRD-LP-GH][" << proxy_ip_port << "] AFTER blk=" << request->local_block_id()
      //           << " disk_hex=" << cord_dbg_hex_preview(ghv.data(), ghv.size()) << std::endl;
    }
    response->set_ifcommit(true);
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::scheduleAppend2Datanode(
      grpc::ServerContext *context,
      const proxy_proto::AppendStripeDataPlacement *append_stripe_data_placement,
      proxy_proto::SetReply *response)
  {
    // printAppendStripeDataPlacement(append_stripe_data_placement);

    int stripe_id = append_stripe_data_placement->stripe_id();
    // sum of all append slices allocated to this proxy
    size_t cluster_append_size = append_stripe_data_placement->append_size();
    // number of slices allocated to this proxy
    int slice_num = append_stripe_data_placement->blockkeys_size();
    bool is_serialized = append_stripe_data_placement->is_serialized();

    auto placement_copy = std::make_shared<proxy_proto::AppendStripeDataPlacement>(*append_stripe_data_placement);

    auto append_and_save = [this, stripe_id, cluster_append_size, slice_num, placement_copy, is_serialized]() mutable
    {
      try
      {
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        // assert(m_pre_allocated_buffer_queue.size() > 0 && "Pre-allocated buffer queue is empty");
        // std::shared_ptr<char[]> append_buf = m_pre_allocated_buffer_queue.front();
        // m_pre_allocated_buffer_queue.pop();
        // char *append_buf = new char[cluster_append_size];
        // memset(append_buf, 0, cluster_append_size);
        // std::shared_ptr<char> append_buf_ptr(append_buf, [](char* p) { delete[] p; }); // 使用智能指针管理内存
        std::vector<char> append_buf(cluster_append_size, 0);
        asio::read(socket_data, asio::buffer(append_buf.data(), cluster_append_size), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Append339]"
                    << "Append to Stripe " << stripe_id << " with length of " << cluster_append_size << std::endl;
        }

        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        std::vector<char *> slices = m_toolbox->splitCharPointer(append_buf.data(), placement_copy);

        auto append_to_datanode = [this](const char *block_key, int block_id, size_t slice_size, const char *slice_buf, int slice_offset, const char *ip, int port, bool is_serialized)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][Append353]"
                      << "Append to Block " << block_key << " of block_id " << block_id << " at the offset of " << slice_offset << " with length of " << slice_size << std::endl;
          }
          AppendToDatanode(block_key, block_id, slice_size, slice_buf, slice_offset, ip, port, is_serialized);
        };

        std::vector<std::thread> senders;
        for (int j = 0; j < slice_num; j++)
        {
          senders.push_back(std::thread(append_to_datanode, placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->sizes(j), slices[j], placement_copy->offsets(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), is_serialized));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Append371]"
                    << "Finish appending to Stripe " << stripe_id << std::endl;
        }

        if (placement_copy->is_merge_parity())
        {
          for (int j = 0; j < slice_num; j++)
          {
            if (placement_copy->blockids(j) >= m_sys_config->k)
            {
              MergeParityOnDatanode(placement_copy->blockkeys(j).c_str(), placement_copy->blockids(j), placement_copy->datanodeip(j).c_str(), placement_copy->datanodeport(j), placement_copy->append_mode());
            }
          }

          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][Append387]"
                      << "Async merging parities of Stripe " << stripe_id << std::endl;
          }
        }

        // report to coordinator
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = APPEND;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(placement_copy->key());
        commit_abort_key.set_stripe_id(stripe_id);
        commit_abort_key.set_ifcommitmetadata(true);
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][APPEND405]"
                    << " report to coordinator success" << std::endl;
        }
        else if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][APPEND410]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in append_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][APPEND424] Handle append_and_save" << std::endl;
      }
      std::thread my_thread(append_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::encodeAndSetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::SetReply *response)
  {
    std::string key = object_and_placement->key();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int stripe_id = object_and_placement->stripe_id();
    int block_size = object_and_placement->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }
    auto encode_and_save = [this, key, value_size_bytes, k, g_m, l, block_size, keys_nodes, encode_type]() mutable
    {
      try
      {
        // read the key and value in the socket sent by client
        // initialize the socket of reading key and value
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        int extend_value_size_byte = block_size * k;
        std::vector<char> buf_key(key.size());
        std::vector<char> v_buf(extend_value_size_byte);
        for (int i = value_size_bytes; i < extend_value_size_byte; i++)
        {
          v_buf[i] = '0';
        }

        // read the key
        asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Check key " << buf_key.data() << std::endl;
        }

        // check the key
        bool flag = true;
        for (int i = 0; i < int(key.size()); i++)
        {
          if (key[i] != buf_key[i])
          {
            flag = false;
          }
        }
        if (flag)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Read value of " << buf_key.data() << std::endl;
          }
          // read the value
          asio::read(socket_data, asio::buffer(v_buf.data(), value_size_bytes), error);
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        // set the blocks to the datanode
        char *buf = v_buf.data();
        // define a lambda function to send to datanode
        auto send_to_datanode = [this](int j, int k, std::string block_key, char **data, char **coding, int block_size, std::pair<std::string, int> ip_and_port)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                      << "Thread " << j << " send " << block_key << " to Datanode" << ip_and_port.second << std::endl;
          }
          if (j < k)
          {
            // send to data node
            SetToDatanode(block_key.c_str(), block_key.size(), data[j], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
          else
          {
            // send to parity node
            SetToDatanode(block_key.c_str(), block_key.size(), coding[j - k], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
        };

        // calculate parity blocks
        // initialize the area of parity blocks
        std::vector<char *> v_data(k);
        std::vector<char *> v_coding(g_m + l + 1);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();

        std::vector<std::vector<char>> v_coding_area(g_m + l + 1, std::vector<char>(block_size));
        for (int j = 0; j < k; j++)
        {
          data[j] = &buf[j * block_size];
        }
        for (int j = 0; j < g_m + l + 1; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Encode value with size of " << v_buf.size() << std::endl;
        }
        int send_num;
        if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
        {
          encode(k, g_m, l, data, coding, block_size, encode_type);
          send_num = k + g_m + l;
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Distribute blocks to datanodes" << std::endl;
        }

        // call the lambda function send_to_datanode to send data and parity blocks
        std::vector<std::thread> senders;
        for (int j = 0; j < send_num; j++)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          senders.push_back(std::thread(send_to_datanode, j, k, block_key, data, coding, block_size, ip_and_port));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "Finish distributing blocks!" << std::endl;
        }

        // report to coordinator
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = SET;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(key);
        commit_abort_key.set_ifcommitmetadata(true);
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << "[SET] report to coordinator success" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][SET]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in encode_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][SET] Handle encode and set" << std::endl;
      }
      std::thread my_thread(encode_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::decodeAndGetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::GetReply *response)
  {
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::string key = object_and_placement->key();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    // int block_size = object_and_placement->block_size();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int block_size = ceil(value_size_bytes, k);
    std::string clientip = object_and_placement->clientip();
    int clientport = object_and_placement->clientport();
    int stripe_id = object_and_placement->stripe_id();

    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    std::vector<int> block_idxs;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      block_idxs.push_back(object_and_placement->blockids(i));
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }

    auto decode_and_get = [this, key, k, g_m, l, block_size, value_size_bytes, stripe_id,
                           clientip, clientport, keys_nodes, block_idxs, encode_type]() mutable
    {
      int expect_block_number = (encode_type == Azure_LRC) ? (k + l) : k;
      int all_expect_blocks = (encode_type == Azure_LRC) ? (k + g_m + l) : (k + g_m);

      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_key_ptr = std::make_shared<std::vector<std::string>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto myLock_ptr = std::make_shared<std::mutex>();
      auto cv_ptr = std::make_shared<std::condition_variable>();

      std::vector<char *> v_data(k);
      std::vector<char *> v_coding(all_expect_blocks - k);
      char **data = v_data.data();
      char **coding = v_coding.data();

      auto getFromNode = [this, k, blocks_ptr, blocks_key_ptr, blocks_idx_ptr, myLock_ptr, cv_ptr](int expect_block_number, int block_idx, std::string block_key, int block_size, std::string ip, int port)
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                    << "Block " << block_idx << " with key " << block_key << " from Datanode" << ip << ":" << port << std::endl;
        }

        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip.c_str(), port, block_idx + 2);

        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        myLock_ptr->lock();
        // get any k blocks and decode
        if (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
        {
          blocks_ptr->push_back(temp);
          blocks_key_ptr->push_back(block_key);
          blocks_idx_ptr->push_back(block_idx);
          if (check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
          {
            cv_ptr->notify_all();
          }
        }
        // get all the blocks
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(block_idx);
        myLock_ptr->unlock();
      };

      std::vector<std::vector<char>> v_data_area(k, std::vector<char>(block_size));
      std::vector<std::vector<char>> v_coding_area(all_expect_blocks - k, std::vector<char>(block_size));
      for (int j = 0; j < k; j++)
      {
        data[j] = v_data_area[j].data();
      }
      for (int j = 0; j < all_expect_blocks - k; j++)
      {
        coding[j] = v_coding_area[j].data();
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to get blocks from datanodes!" << std::endl;
      }
      std::vector<std::thread> read_treads;
      // for (int j = 0; j < k; j++)
      for (int j = 0; j < all_expect_blocks; j++)
      {
        int block_idx = block_idxs[j];
        std::string block_key = keys_nodes[j].first;
        std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
        // std::vector<char> temp(block_size);
        // GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
        // blocks_ptr->push_back(temp);
        // blocks_key_ptr->push_back(block_key);
        // blocks_idx_ptr->push_back(j);
        read_treads.push_back(std::thread(getFromNode, expect_block_number, block_idx, block_key, block_size, ip_and_port.first, ip_and_port.second));
      }
      for (int j = 0; j < all_expect_blocks; j++)
      {
        read_treads[j].detach();
        // read_treads[j].join();
      }

      std::unique_lock<std::mutex> lck(*myLock_ptr);
      while (!check_received_block(k, expect_block_number, blocks_idx_ptr, blocks_ptr->size()))
      {
        cv_ptr->wait(lck);
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "ready to decode!" << std::endl;
      }
      for (int j = 0; j < int(blocks_idx_ptr->size()); j++)
      {
        int idx = (*blocks_idx_ptr)[j];
        if (idx < k)
        {
          memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
        }
        else
        {
          memcpy(coding[idx - k], (*blocks_ptr)[j].data(), block_size);
        }
      }

      auto erasures = std::make_shared<std::vector<int>>();
      for (int j = 0; j < all_expect_blocks; j++)
      {
        if (std::find(blocks_idx_ptr->begin(), blocks_idx_ptr->end(), j) == blocks_idx_ptr->end())
        {
          erasures->push_back(j);
        }
      }
      erasures->push_back(-1);
      if (encode_type == Azure_LRC)
      {
        if (!decode(k, g_m, l, data, coding, erasures, block_size, encode_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy cannot decode!" << std::endl;
        }
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET] proxy decode error!" << std::endl;
      }
      std::string value;
      for (int j = 0; j < k; j++)
      {
        value += std::string(data[j]);
      }

      if (IF_DEBUG)
      {
        std::cout << "\033[1;31m[Proxy" << m_self_cluster_id << "][GET]"
                  << "send " << key << " to client with length of " << value.size() << "\033[0m" << std::endl;
      }

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(clientip, std::to_string(clientport));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(key, key.size()), error);
      if(error)
      {
        std::cout << "error in write key" << std::endl;
      }
      asio::write(sock_data, asio::buffer(value, value_size_bytes), error);
      if(error)
      {
        std::cout << "error in write value" << std::endl;
      }
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    };
    try
    {
      // std::cerr << "decode_and_get_thread start" << std::endl;
      if (IF_DEBUG)
      {
        std::cout << "[Proxy] Handle get and decode" << std::endl;
      }
      std::thread my_thread(decode_and_get);
      my_thread.detach();
      // std::cerr << "decode_and_get_thread detach" << std::endl;
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  std::vector<unsigned char *> ProxyImpl::convertToUnsignedCharArray(std::vector<char*> &input)
  {
    std::vector<unsigned char *> output;

    for (auto &row : input)
    {
      output.push_back(reinterpret_cast<unsigned char *>(row));
    }

    return output;
  }

  void ProxyImpl::get_from_node(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index)
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "Block key " << block_key << " from Datanode" << datanode_ip << ":" << datanode_port << std::endl;
    }
    status[index] = GetFromDatanode(block_key, block_value, block_size, datanode_ip, datanode_port);
  }

  void ProxyImpl::get_from_node_breakdown(const std::string &block_key, char *block_value, const size_t block_size, const char *datanode_ip, const int datanode_port, bool *status, int index, 
    double *disk_io_start_time, double *disk_io_end_time, double *network_start_time, double *network_end_time, double *grpc_notify_time, double *grpc_start_time)
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "Block key " << block_key << " from Datanode" << datanode_ip << ":" << datanode_port << std::endl;
    }
    status[index] = GetFromDatanode(block_key, block_value, block_size, datanode_ip, datanode_port, disk_io_start_time, disk_io_end_time, network_start_time, network_end_time, grpc_notify_time, grpc_start_time);
  }
  // degraded read
  grpc::Status ProxyImpl::degradedRead(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response)
  {
    auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

    auto degraded_read = [this, request_copy, &response]() mutable
    {
      std::string code_type = m_sys_config->CodeType;
      // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
      std::fill_n(status.get(), request_copy->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(request_copy->datanodeip_size());

      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }

      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      std::vector<std::thread> get_threads;
      //std::chrono::high_resolution_clock::time_point t0 = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this,
                                          request_copy->blockkeys(i), // block key
                                          get_bufs[i],               // buffer to store the block value
                                          m_sys_config->BlockSize,   // block size
                                          request_copy->datanodeip(i).c_str(), // datanode IP
                                          request_copy->datanodeport(i),       // datanode port
                                          status.get(),              // status array to track success/failure
                                          i                          // index for status array
        ));
      }
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          block_idxs.push_back(request_copy->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);
        if (code_type == "UniLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
        }
        else if (code_type == "OptimalLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
        }
        else if (code_type == "UniformLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::string client_ip = request_copy->clientip();
        int client_port = request_copy->clientport();

        // send to the client
        asio::error_code error;
        asio::ip::tcp::resolver resolver(io_context);
        asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
        //asio::io_context io_context;
        asio::ip::tcp::socket socket_data(io_context);
        asio::connect(socket_data, endpoints);
        //acceptor.accept(socket_data);
        if (error)
        {
          std::cout << "error in connect" << std::endl;
        }
        asio::write(socket_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
        if (error)
        {
          std::cout << "error in write" << std::endl;
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
        socket_data.close(ignore_ec);
        delete res_buf;
        for(int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          delete get_bufs[i];
        }
        //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
      }
    };

    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
      }

      std::thread my_thread(degraded_read);
      my_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::degradedReadBreakdown(
      grpc::ServerContext *context,
      const proxy_proto::DegradedReadRequest *degraded_read_request,
      proxy_proto::DegradedReadReply *response)
  {
    std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
    response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());

    auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

    auto degraded_read = [this, request_copy, &response]() mutable
    {
      std::string code_type = m_sys_config->CodeType;
      // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
      std::fill_n(status.get(), request_copy->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(request_copy->datanodeip_size());
      std::vector<double> data_node_disk_io_start_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_disk_io_end_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_start_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_end_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_notify_time(request_copy->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_start_time(request_copy->datanodeip_size(), 0.0);
      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }

      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      std::vector<std::thread> get_threads;
      //std::chrono::high_resolution_clock::time_point t0 = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this,
                                          request_copy->blockkeys(i), // block key
                                          get_bufs[i],               // buffer to store the block value
                                          m_sys_config->BlockSize,   // block size
                                          request_copy->datanodeip(i).c_str(), // datanode IP
                                          request_copy->datanodeport(i),       // datanode port
                                          status.get(),              // status array to track success/failure
                                          i,                         // index for status array
                                          &data_node_disk_io_start_time[i],         // pointer to store disk IO time
                                          &data_node_disk_io_end_time[i],           // pointer to store disk IO time
                                          &data_node_network_start_time[i],          // pointer to store network time
                                          &data_node_network_end_time[i],            // pointer to store network time
                                          &data_node_grpc_notify_time[i],            // pointer to store grpc notify time
                                          &data_node_grpc_start_time[i]              // pointer to store grpc start time
        ));
      }
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
      //std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0);
      bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        //response->set_disk_io_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()) - *std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
        //response->set_network_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()) - *std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
        response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
        response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
        response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
        response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
        response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
        response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          block_idxs.push_back(request_copy->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        if (code_type == "UniLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
        }
        else if (code_type == "OptimalLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
        }
        else if (code_type == "UniformLRC")
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
        response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(t2.time_since_epoch()).count());
        response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(t3.time_since_epoch()).count());
        std::string client_ip = request_copy->clientip();
        int client_port = request_copy->clientport();

        // send to the client
        asio::error_code error;
        asio::ip::tcp::resolver resolver(io_context);
        asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
        asio::ip::tcp::socket sock_data(io_context);
        //asio::connect(sock_data, endpoints);
        sock_data.connect(*endpoints, error);
        if (error)
        {
          std::cout << "error in connect" << std::endl;
        }
        asio::write(sock_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
        if (error)
        {
          std::cout << "error in write" << std::endl;
        }
        asio::error_code ignore_ec;
        sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
        sock_data.close(ignore_ec);
        delete res_buf;
        for(int i = 0; i < request_copy->datanodeip_size(); i++)
        {
          delete get_bufs[i];
        }
        //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
      }
    };

    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
      }

      std::thread my_thread(degraded_read);
      my_thread.join();
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::degradedReadWithBlockStripeID(
    grpc::ServerContext *context,
    const proxy_proto::DegradedReadRequest *degraded_read_request,
    proxy_proto::GetReply *response)
{
  auto request_copy = std::make_shared<proxy_proto::DegradedReadRequest>(*degraded_read_request);

  auto degraded_read = [this, request_copy]() mutable
  {
    int stripe_id = request_copy->failed_block_stripe_id();
    std::string code_type = m_sys_config->CodeType;
    // auto status = std::make_shared<std::vector<bool>>(request_copy->datanodeip_size(), false);
    std::unique_ptr<bool[]> status(new bool[request_copy->datanodeip_size()]);
    std::fill_n(status.get(), request_copy->datanodeip_size(), false);

    //std::vector<std::vector<char>> get_bufs(request_copy->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
    std::vector<char*> get_bufs(request_copy->datanodeip_size());
    for(int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    for (int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, request_copy->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, request_copy->datanodeip(i).c_str(), request_copy->datanodeport(i), status.get(), i));
    }
    for (int i = 0; i < request_copy->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + request_copy->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }
    else
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;

      std::vector<int> block_idxs;
      for (int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        block_idxs.push_back(request_copy->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
      if (code_type == "UniLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_unilrc" << std::endl;
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc" << std::endl;
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_azure_lrc success!" << std::endl;
      }
      else if (code_type == "OptimalLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc" << std::endl;
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_optimal_lrc success!" << std::endl;
      }
      else if (code_type == "UniformLRC")
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc" << " failed block id: " << request_copy->failed_block_id() << std::endl;
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, request_copy->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, request_copy->failed_block_id());
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode_uniform_lrc success!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }

      std::string client_ip = request_copy->clientip();
      int client_port = request_copy->clientport();

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(client_ip, std::to_string(client_port));
      asio::ip::tcp::socket sock_data(io_context);
      //asio::connect(sock_data, endpoints);
      sock_data.connect(*endpoints, error);
      if (error)
      {
        std::cout << "error in connect" << std::endl;
      }
      asio::write(sock_data, asio::buffer(&stripe_id, sizeof(int)), error);
      asio::write(sock_data, asio::buffer(res_buf, m_sys_config->BlockSize), error);
      if (error)
      {
        std::cout << "error in write" << std::endl;
      }
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
      delete res_buf;
      for(int i = 0; i < request_copy->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
      //std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] send to the client done" << std::endl;
    }
  };

  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] Handle degraded read" << std::endl;
    }

    std::thread my_thread(degraded_read);
    my_thread.detach();
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }

  return grpc::Status::OK;
}

  grpc::Status ProxyImpl::degradedRead2Client(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::DegradedReadReply *response)
{
  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle Degraded Read" << std::endl;
    }

    std::string code_type = m_sys_config->CodeType;
    int cross_rack_num = recovery_request->cross_rack_num();
    std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
    std::fill_n(status.get(), recovery_request->datanodeip_size(), false);
    std::vector<char*> get_bufs(recovery_request->datanodeip_size());
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, 
      recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(), i));
    }
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }

    else
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;
      std::vector<int> block_idxs;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        block_idxs.push_back(recovery_request->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::string failed_block_key = recovery_request->failed_block_key();
      int failed_block_id = recovery_request->failed_block_id();
      std::string replaced_node_ip = recovery_request->replaced_node_ip();
      int replaced_node_port = recovery_request->replaced_node_port();

      if (code_type == "UniLRC")
      {
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "OptimalLRC")
      {
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "UniformLRC")
      {
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }
      if(cross_rack_num){
        std::cout << "start to recover cross rack" << std::endl;
        char **cross_rack_bufs = new char*[cross_rack_num];
        for(int i = 0; i < cross_rack_num; i++)
        {
          cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
        }
        std::vector<std::thread> get_from_proxies_threads;
        std::vector<double> accept_start_time(cross_rack_num, 0.0);
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs]()mutable{
            //asio::io_context io_context;
            asio::ip::tcp::socket socket(this->io_context);
            //asio::ip::tcp::resolver resolver(io_context);
            this->acceptor.accept(socket);
            std::cout << "connected to porxy" << std::endl;
            asio::error_code error;
            size_t len = asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
            if(len != this->m_sys_config->BlockSize)
            {
              std::cout << "error in read" << std::endl;
            }
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
            socket.close(ignore_ec);
          }));
        }
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads[i].join();
        }
        std::cout << "start to xor" << std::endl;
        char **buf_ptrs = new char*[cross_rack_num + 2];
        for(int i = 0; i < cross_rack_num; i++)
        {
          buf_ptrs[i] = cross_rack_bufs[i];
        }
        buf_ptrs[cross_rack_num] = res_buf;
        buf_ptrs[cross_rack_num + 1] = real_res_buf;
        xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
        for(int i = 0; i < cross_rack_num; i++)
        {
          delete cross_rack_bufs[i];
        }
        delete cross_rack_bufs;
        delete buf_ptrs;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
      }
    }
  
    std::string replaced_node_ip = recovery_request->replaced_node_ip();
    int replaced_node_port = recovery_request->replaced_node_port();
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded] send to the client" << replaced_node_ip << ":" << replaced_node_port << std::endl;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(replaced_node_ip, std::to_string(replaced_node_port));
    asio::connect(socket, endpoints);
    if(recovery_request->is_to_send_block_id()){
      int32_t block_id_to_send = recovery_request->block_id_to_send();
      asio::write(socket, asio::buffer(&block_id_to_send, sizeof(int32_t)));
    }
    if(cross_rack_num){
      asio::write(socket, asio::buffer(real_res_buf, m_sys_config->BlockSize));
    }
    else{
      asio::write(socket, asio::buffer(res_buf, m_sys_config->BlockSize));
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded Read] send to the client done" << std::endl;
    delete res_buf;
    delete real_res_buf;
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      delete get_bufs[i];
    }
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }
  return grpc::Status::OK;
}


  grpc::Status ProxyImpl::degradedRead2ClientBreakdown(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::DegradedReadReply *response)
{
  std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
  response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
  try
  {
    if (IF_DEBUG)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle Degraded Read" << std::endl;
    }

    std::string code_type = m_sys_config->CodeType;
    int cross_rack_num = recovery_request->cross_rack_num();
    std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
    std::fill_n(status.get(), recovery_request->datanodeip_size(), false);


    std::vector<double> data_node_disk_io_start_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_disk_io_end_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_network_start_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_network_end_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_grpc_notify_time(recovery_request->datanodeip_size(), 0.0);
    std::vector<double> data_node_grpc_start_time(recovery_request->datanodeip_size(), 0.0);

    std::vector<char*> get_bufs(recovery_request->datanodeip_size());
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    }

    char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
    std::vector<std::thread> get_threads;
    //std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this, recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), recovery_request->datanodeport(i), status.get(), i, 
        &data_node_disk_io_start_time[i], &data_node_disk_io_end_time[i], &data_node_network_start_time[i], &data_node_network_end_time[i], &data_node_grpc_notify_time[i], &data_node_grpc_start_time[i]));
    }
    for (int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      get_threads[i].join();
    }

    bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                { return val == true; });
    if (!all_true)
    {
      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes failed!" << std::endl;
    }

    else
    {
      //response->set_disk_io_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()) - *std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      //response->set_network_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()) - *std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
      response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
      response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
      response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));

      std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                << "read from datanodes success!" << std::endl;
      std::chrono::high_resolution_clock::time_point decode_start_time = std::chrono::high_resolution_clock::now();
      std::vector<int> block_idxs;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        block_idxs.push_back(recovery_request->blockids(i));
      }
      std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

      std::string failed_block_key = recovery_request->failed_block_key();
      int failed_block_id = recovery_request->failed_block_id();
      std::string replaced_node_ip = recovery_request->replaced_node_ip();
      int replaced_node_port = recovery_request->replaced_node_port();

      if (code_type == "UniLRC")
      {
        decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
      }
      else if (is_azure_like_code(code_type))
      {
        decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "OptimalLRC")
      {
        decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else if (code_type == "UniformLRC")
      {
        decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
        exit(1);
      }
      std::chrono::high_resolution_clock::time_point decode_end_time = std::chrono::high_resolution_clock::now();
      response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(decode_start_time.time_since_epoch()).count());
      response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(decode_end_time.time_since_epoch()).count());

      if(cross_rack_num){
        std::cout << "start to recover cross rack" << std::endl;
        char **cross_rack_bufs = new char*[cross_rack_num];
        for(int i = 0; i < cross_rack_num; i++)
        {
          cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
        }
        std::vector<std::thread> get_from_proxies_threads;
        std::vector<double> accept_start_time(cross_rack_num, 0.0);
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs, &accept_start_time]()mutable{
            //asio::io_context io_context;
            asio::ip::tcp::socket socket(this->io_context);
            //asio::ip::tcp::resolver resolver(io_context);
            this->acceptor.accept(socket);
            std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
            accept_start_time[i] = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
            std::cout << "connected to porxy" << std::endl;
            asio::error_code error;
            size_t len = asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
            if(len != this->m_sys_config->BlockSize)
            {
              std::cout << "error in read" << std::endl;
            }
            asio::error_code ignore_ec;
            socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
            socket.close(ignore_ec);
          }));
        }
        for(int i = 0; i < cross_rack_num; i++)
        {
          get_from_proxies_threads[i].join();
        }
        double min_accept_start_time = *std::min_element(accept_start_time.begin(), accept_start_time.end());
        std::chrono::high_resolution_clock::time_point accept_end_time = std::chrono::high_resolution_clock::now();
        double time_span3 = std::chrono::duration_cast<std::chrono::duration<double>>(accept_end_time.time_since_epoch()).count() - min_accept_start_time;
        response->set_cross_rack_time(time_span3);
        std::cout << "start to xor" << std::endl;
        char **buf_ptrs = new char*[cross_rack_num + 2];
        for(int i = 0; i < cross_rack_num; i++)
        {
          buf_ptrs[i] = cross_rack_bufs[i];
        }
        buf_ptrs[cross_rack_num] = res_buf;
        buf_ptrs[cross_rack_num + 1] = real_res_buf;
        std::chrono::high_resolution_clock::time_point cross_rack_xor_start_time = std::chrono::high_resolution_clock::now();
        xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
        std::chrono::high_resolution_clock::time_point cross_rack_xor_end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> xor_time = std::chrono::duration_cast<std::chrono::duration<double>>(cross_rack_xor_end_time - cross_rack_xor_start_time);
        response->set_cross_rack_xor_time(xor_time.count());
        for(int i = 0; i < cross_rack_num; i++)
        {
          delete cross_rack_bufs[i];
        }
        delete cross_rack_bufs;
        delete buf_ptrs;
      }
      else
      {
        response->set_cross_rack_time(0);
        response->set_cross_rack_xor_time(0);
        std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
      }
    }
  
    std::string replaced_node_ip = recovery_request->replaced_node_ip();
    int replaced_node_port = recovery_request->replaced_node_port();
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded] send to the client" << replaced_node_ip << ":" << replaced_node_port << std::endl;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(replaced_node_ip, std::to_string(replaced_node_port));
    asio::connect(socket, endpoints);
    if(cross_rack_num){
      asio::write(socket, asio::buffer(real_res_buf, m_sys_config->BlockSize));
    }
    else{
      asio::write(socket, asio::buffer(res_buf, m_sys_config->BlockSize));
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    std::cout << "[Proxy" << m_self_cluster_id << "][Degraded Read] send to the client done" << std::endl;
    delete res_buf;
    delete real_res_buf;
    for(int i = 0; i < recovery_request->datanodeip_size(); i++)
    {
      delete get_bufs[i];
    }
  }
  catch (const std::exception &e)
  {
    std::cout << "exception" << std::endl;
    std::cerr << e.what() << '\n';
  }
  return grpc::Status::OK;
}


  // recovery
  grpc::Status ProxyImpl::recovery(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::RecoveryReply *response)
  {
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle recovery" << std::endl;
      }

      std::string code_type = m_sys_config->CodeType;
      int cross_rack_num = recovery_request->cross_rack_num();
      // auto status = std::make_shared<std::vector<bool>>(recovery_request->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
      std::fill_n(status.get(), recovery_request->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(recovery_request->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(recovery_request->datanodeip_size());
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }
      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));

      std::vector<std::thread> get_threads;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node, this, 
          recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), 
          recovery_request->datanodeport(i), status.get(), i));
      }
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }
      
      bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < recovery_request->datanodeip_size(); i++)
        {
          block_idxs.push_back(recovery_request->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::string failed_block_key = recovery_request->failed_block_key();
        int failed_block_id = recovery_request->failed_block_id();
        std::string replaced_node_ip = recovery_request->replaced_node_ip();
        int replaced_node_port = recovery_request->replaced_node_port();

        if (code_type == "UniLRC")
        {
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "OptimalLRC")
        {
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "UniformLRC")
        {
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }

        if(cross_rack_num){
          std::cout << "start to recover cross rack" << std::endl;
          char **cross_rack_bufs = new char*[cross_rack_num];
          for(int i = 0; i < cross_rack_num; i++)
          {
            cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
          }
          std::vector<std::thread> get_from_proxies_threads;
          std::vector<std::string> cross_rack_ips;
          //std::vector<int> cross_rack_ports;
          //for(int i = 0; i < cross_rack_num; i++)
          //{
          //  cross_rack_ips.push_back(recovery_request->proxyip(i));
          //  cross_rack_ports.push_back(recovery_request->proxyport(i));
          //}
          //std::lock_guard<std::mutex> lock(m_mutex);
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs]()mutable{
              asio::ip::tcp::socket socket(this->io_context);
              std::cout << "connecting to proxy" << std::endl;
              this->acceptor.accept(socket);
              std::cout << "connected to porxy" << std::endl;
              asio::error_code error;
              asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
              std::cout << "read from proxy"  << std::endl;
              if(error)
              {
                std::cout << "error in read" << std::endl;
              }
              asio::error_code ignore_ec;
              socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
              socket.close(ignore_ec);
            }));
          }
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads[i].join();
          }

          std::cout << "start to xor" << std::endl;
          char **buf_ptrs = new char*[cross_rack_num + 2];
          for(int i = 0; i < cross_rack_num; i++)
          {
            buf_ptrs[i] = cross_rack_bufs[i];
          }
          buf_ptrs[cross_rack_num] = res_buf;
          buf_ptrs[cross_rack_num + 1] = real_res_buf;
          xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
          for(int i = 0; i < cross_rack_num; i++)
          {
            delete cross_rack_bufs[i];
          }
          delete cross_rack_bufs;
          delete buf_ptrs;
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
        }
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] send to the replaced node" << std::endl;
        // send to the replaced node
        if(cross_rack_num){
          RecoveryToDatanode(failed_block_key.c_str(), failed_block_id, real_res_buf, replaced_node_ip.c_str(), replaced_node_port);
        }
        else{
          RecoveryToDatanode(failed_block_key.c_str(), failed_block_id, res_buf, replaced_node_ip.c_str(), replaced_node_port);
        }
      }
      delete res_buf;
      delete real_res_buf;
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::recoveryBreakdown(
    grpc::ServerContext *context,
    const proxy_proto::RecoveryRequest *recovery_request,
    proxy_proto::RecoveryReply *response)
  {
    std::chrono::high_resolution_clock::time_point START = std::chrono::high_resolution_clock::now();
    response->set_grpc_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(START.time_since_epoch()).count());
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] Handle recovery" << std::endl;
      }

      std::string code_type = m_sys_config->CodeType;
      int cross_rack_num = recovery_request->cross_rack_num();
      // auto status = std::make_shared<std::vector<bool>>(recovery_request->datanodeip_size(), false);
      std::unique_ptr<bool[]> status(new bool[recovery_request->datanodeip_size()]);
      std::fill_n(status.get(), recovery_request->datanodeip_size(), false);

      //std::vector<std::vector<char>> get_bufs(recovery_request->datanodeip_size(), std::vector<char>(m_sys_config->BlockSize, 0));
      std::vector<char*> get_bufs(recovery_request->datanodeip_size());
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      }
      //std::vector<char> res_buf(m_sys_config->BlockSize, 0);
      char *res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
      char *real_res_buf = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));

      std::vector<double> data_node_disk_io_start_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_disk_io_end_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_start_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_network_end_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_notify_time(recovery_request->datanodeip_size(), 0.0);
      std::vector<double> data_node_grpc_start_time(recovery_request->datanodeip_size(), 0.0);

      std::vector<std::thread> get_threads;
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads.push_back(std::thread(&ProxyImpl::get_from_node_breakdown, this, 
          recovery_request->blockkeys(i), get_bufs[i], m_sys_config->BlockSize, recovery_request->datanodeip(i).c_str(), 
          recovery_request->datanodeport(i), status.get(), i, &data_node_disk_io_start_time[i], &data_node_disk_io_end_time[i],
          &data_node_network_start_time[i], &data_node_network_end_time[i], &data_node_grpc_notify_time[i], &data_node_grpc_start_time[i]));
      }
      for (int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        get_threads[i].join();
      }

      response->set_disk_io_start_time(*std::min_element(data_node_disk_io_start_time.begin(), data_node_disk_io_start_time.end()));
      response->set_disk_io_end_time(*std::max_element(data_node_disk_io_end_time.begin(), data_node_disk_io_end_time.end()));
      response->set_network_start_time(*std::min_element(data_node_network_start_time.begin(), data_node_network_start_time.end()));
      response->set_network_end_time(*std::max_element(data_node_network_end_time.begin(), data_node_network_end_time.end()));
      response->set_data_node_grpc_notify_time(*std::min_element(data_node_grpc_notify_time.begin(), data_node_grpc_notify_time.end()));
      response->set_data_node_grpc_start_time(*std::max_element(data_node_grpc_start_time.begin(), data_node_grpc_start_time.end()));
      
      bool all_true = std::all_of(status.get(), status.get() + recovery_request->datanodeip_size(), [](bool val)
                                  { return val == true; });
      if (!all_true)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes failed!" << std::endl;
      }
      else
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][GET]"
                  << "read from datanodes success!" << std::endl;

        std::vector<int> block_idxs;
        for (int i = 0; i < recovery_request->datanodeip_size(); i++)
        {
          block_idxs.push_back(recovery_request->blockids(i));
        }
        std::vector<unsigned char *> block_ptrs = convertToUnsignedCharArray(get_bufs);

        std::string failed_block_key = recovery_request->failed_block_key();
        int failed_block_id = recovery_request->failed_block_id();
        std::string replaced_node_ip = recovery_request->replaced_node_ip();
        int replaced_node_port = recovery_request->replaced_node_port();

        std::chrono::high_resolution_clock::time_point t3 = std::chrono::high_resolution_clock::now();
        if (code_type == "UniLRC")
        {
          decode_unilrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize);
        }
        else if (is_azure_like_code(code_type))
        {
          decode_azure_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "OptimalLRC")
        {
          decode_optimal_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else if (code_type == "UniformLRC")
        {
          decode_uniform_lrc(m_sys_config->k, m_sys_config->r, m_sys_config->z, recovery_request->datanodeip_size(), &block_idxs, block_ptrs.data(), reinterpret_cast<unsigned char *>(res_buf), m_sys_config->BlockSize, failed_block_id);
        }
        else
        {
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] code type error!" << std::endl;
          exit(1);
        }
        std::chrono::high_resolution_clock::time_point t4 = std::chrono::high_resolution_clock::now();
        response->set_decode_start_time(std::chrono::duration_cast<std::chrono::duration<double>>(t3.time_since_epoch()).count());
        response->set_decode_end_time(std::chrono::duration_cast<std::chrono::duration<double>>(t4.time_since_epoch()).count());

        if(cross_rack_num){
          std::cout << "start to recover cross rack" << std::endl;
          char **cross_rack_bufs = new char*[cross_rack_num];
          for(int i = 0; i < cross_rack_num; i++)
          {
            cross_rack_bufs[i] = static_cast<char*>(std::aligned_alloc(32, m_sys_config->BlockSize));
          }
          std::vector<std::thread> get_from_proxies_threads;
          std::vector<double> accept_start_time(cross_rack_num, 0.0);
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads.push_back(std::thread([i, this, &cross_rack_bufs, &accept_start_time]()mutable{
              //asio::io_context io_context;
              asio::ip::tcp::socket socket(this->io_context);
              //asio::ip::tcp::resolver resolver(io_context);
              std::cout << "connecting to proxy" << std::endl;
              this->acceptor.accept(socket);
              std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
              accept_start_time[i] = std::chrono::duration_cast<std::chrono::duration<double>>(start.time_since_epoch()).count();
              std::cout << "connected to porxy" << std::endl;
              asio::error_code error;
              asio::read(socket, asio::buffer(cross_rack_bufs[i], this->m_sys_config->BlockSize), error);
              std::cout << "read from proxy"  << std::endl;
              if(error)
              {
                std::cout << "error in read" << std::endl;
              }
              asio::error_code ignore_ec;
              socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
              socket.close(ignore_ec);
            }));
          }
          for(int i = 0; i < cross_rack_num; i++)
          {
            get_from_proxies_threads[i].join();
          }
          double min_accept_start_time = *std::min_element(accept_start_time.begin(), accept_start_time.end());
          std::chrono::high_resolution_clock::time_point accept_end_time = std::chrono::high_resolution_clock::now();
          double time_span3 = std::chrono::duration_cast<std::chrono::duration<double>>(accept_end_time.time_since_epoch()).count() - min_accept_start_time;
          response->set_cross_rack_time(time_span3);
          std::cout << "start to xor" << std::endl;
          char **buf_ptrs = new char*[cross_rack_num + 2];
          for(int i = 0; i < cross_rack_num; i++)
          {
            buf_ptrs[i] = cross_rack_bufs[i];
          }
          buf_ptrs[cross_rack_num] = res_buf;
          buf_ptrs[cross_rack_num + 1] = real_res_buf;
          std::chrono::high_resolution_clock::time_point t7 = std::chrono::high_resolution_clock::now();
          xor_avx(cross_rack_num + 2, m_sys_config->BlockSize, (void**)buf_ptrs);
          std::chrono::high_resolution_clock::time_point t8 = std::chrono::high_resolution_clock::now();
          std::chrono::duration<double> time_span4 = std::chrono::duration_cast<std::chrono::duration<double>>(t8 - t7);
          response->set_cross_rack_xor_time(time_span4.count());
          for(int i = 0; i < cross_rack_num; i++)
          {
            delete cross_rack_bufs[i];
          }
          delete cross_rack_bufs;
          delete buf_ptrs;
        }
        else
        {
          response->set_cross_rack_time(0);
          response->set_cross_rack_xor_time(0);
          std::cout << "[Proxy" << m_self_cluster_id << "][Degrade read] decode success!" << std::endl;
        }
        std::cout << "[Proxy" << m_self_cluster_id << "][Recovery] send to the replaced node" << std::endl;
        // send to the replaced node
        double dest_data_node_network_time, dest_data_node_disk_io_time;
        if(cross_rack_num){
          RecoveryToDatanodeBreakdown(failed_block_key.c_str(), failed_block_id, real_res_buf, replaced_node_ip.c_str(), replaced_node_port, 
            &dest_data_node_network_time, &dest_data_node_disk_io_time);
        }
        else{
          RecoveryToDatanodeBreakdown(failed_block_key.c_str(), failed_block_id, res_buf, replaced_node_ip.c_str(), replaced_node_port, 
            &dest_data_node_network_time, &dest_data_node_disk_io_time);
        }
        response->set_dest_data_node_network_time(dest_data_node_network_time);
        response->set_dest_data_node_disk_io_time(dest_data_node_disk_io_time);
      }
      delete res_buf;
      delete real_res_buf;
      for(int i = 0; i < recovery_request->datanodeip_size(); i++)
      {
        delete get_bufs[i];
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cerr << e.what() << '\n';
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::partialDecoding(
    grpc::ServerContext *context,
    const proxy_proto::PartialDecodingRequest *partial_decoding_request,
    proxy_proto::DegradedReadReply *response)
  {
    int block_size = m_sys_config->BlockSize;
    int source_num = partial_decoding_request->source_block_ids_size();
    int decode_num = partial_decoding_request->decode_num();
    unsigned char *source_buf_ptrs[source_num];
    unsigned char *decode_buf_ptrs[decode_num];
    for(int i = 0; i < source_num; i++)
    {
      source_buf_ptrs[i] = static_cast<unsigned char*>(std::aligned_alloc(32, block_size));
    }
    std::vector<std::thread> get_threads;
    for(int i = 0; i < source_num; i++)
    {
      get_threads.push_back(std::thread([this, i, &partial_decoding_request, &source_buf_ptrs, block_size]() {
        this->GetFromDatanode(
            partial_decoding_request->source_block_keys(i), 
            reinterpret_cast<char*>(source_buf_ptrs[i]),
            static_cast<size_t>(block_size),
            partial_decoding_request->source_datanode_ips(i).c_str(),
            static_cast<int>(partial_decoding_request->source_datanode_ports(i))
        );
      }));
    }
    for(int i = 0; i < decode_num; i++)
    {
      decode_buf_ptrs[i] = static_cast<unsigned char*>(std::aligned_alloc(32, block_size));
    }
    unsigned char decode_matrix[source_num * decode_num];
    for(int i = 0; i < source_num * decode_num; i++)
    {
      decode_matrix[i] = partial_decoding_request->decode_factors(i);
    }
    for(int i = 0; i < source_num; i++)
    {
      get_threads[i].join();
    }
    ECProject::encode_data(block_size, source_num, decode_num, decode_matrix, source_buf_ptrs, decode_buf_ptrs);
    std::string dest_proxy_ip = partial_decoding_request->dest_ip();
    int dest_proxy_port = partial_decoding_request->dest_port();
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(dest_proxy_ip, std::to_string(dest_proxy_port));
    asio::connect(socket, endpoints);
    for(int i = 0; i < decode_num; i++)
    {
      asio::write(socket, asio::buffer(decode_buf_ptrs[i], block_size));
      delete decode_buf_ptrs[i];
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    for(int i = 0; i < source_num; i++)
    {
      delete source_buf_ptrs[i];
    }
    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::multipleRecovery(
    grpc::ServerContext *context,
    const proxy_proto::MultipleRecoveryRequest *multiple_recovery_request,
    proxy_proto::GetReply *response)
  {
  try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_cluster_id << "][MultipleRecovery] Handle multiple recovery" << std::endl;
      }

      int resource_proxy_num = multiple_recovery_request->cross_rack_num();
      int block_num = multiple_recovery_request->failed_block_keys_size();
      int block_size = m_sys_config->BlockSize;
      std::string replacing_node_ip = multiple_recovery_request->replacing_node_ip();
      int replacing_node_port = multiple_recovery_request->replacing_node_port();

      // collect failed block keys (order matters: 横向放置顺序)
      std::vector<std::string> block_keys;
      for (int i = 0; i < block_num; ++i)
        block_keys.push_back(multiple_recovery_request->failed_block_keys(i));

      if (resource_proxy_num <= 0 || block_num <= 0)
      {
        std::cerr << "[MultipleRecovery] invalid parameters: resource_proxy_num=" << resource_proxy_num << " block_num=" << block_num << std::endl;
        return grpc::Status::OK;
      }

      size_t per_proxy_len = static_cast<size_t>(block_num) * static_cast<size_t>(block_size);

      // allocate buffers to receive contiguous data from each resource proxy
      char **proxy_bufs = new char *[resource_proxy_num];
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        proxy_bufs[i] = static_cast<char *>(std::aligned_alloc(32, per_proxy_len));
        memset(proxy_bufs[i], 0, per_proxy_len);
      }

      // accept resource_proxy_num connections and read their contiguous payloads
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        try
        {
          asio::ip::tcp::socket socket(this->io_context);
          this->acceptor.accept(socket);
          asio::error_code ec;
          size_t read_bytes = 0;
          while (read_bytes < per_proxy_len)
          {
            read_bytes += asio::read(socket, asio::buffer(proxy_bufs[i] + read_bytes, per_proxy_len - read_bytes), ec);
            if (ec && ec != asio::error::eof)
            {
              std::cerr << "[MultipleRecovery] read error from proxy #" << i << " : " << ec.message() << std::endl;
              break;
            }
            if (ec == asio::error::eof) break;
          }
          asio::error_code ignore_ec;
          socket.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
          socket.close(ignore_ec);
          if (IF_DEBUG)
            std::cout << "[MultipleRecovery] received " << read_bytes << " bytes from proxy #" << i << std::endl;
        }
        catch (const std::exception &e)
        {
          std::cerr << "[MultipleRecovery] exception while receiving from proxy #" << i << " : " << e.what() << std::endl;
        }
      }

      // prepare per-block output buffers and compute XOR across proxies for each block slice
      char **out_blocks = new char *[block_num];
      for (int b = 0; b < block_num; ++b)
      {
        out_blocks[b] = static_cast<char *>(std::aligned_alloc(32, block_size));
        // initialize to zero
        memset(out_blocks[b], 0, block_size);

        // XOR all proxy buffers' slice b into out_blocks[b]
        for (int p = 0; p < resource_proxy_num; ++p)
        {
          char *src = proxy_bufs[p] + static_cast<size_t>(b) * block_size;
          // use 64-bit XOR when possible for speed
          size_t t = 0;
          const size_t WORD_SZ = sizeof(uint64_t);
          for (; t + WORD_SZ <= static_cast<size_t>(block_size); t += WORD_SZ)
          {
            uint64_t *dstw = reinterpret_cast<uint64_t *>(out_blocks[b] + t);
            uint64_t *srcw = reinterpret_cast<uint64_t *>(src + t);
            *dstw ^= *srcw;
          }
          // tail bytes
          for (; t < static_cast<size_t>(block_size); ++t)
            out_blocks[b][t] ^= src[t];
        }
      }

      // send each recovered block to the replacing datanode
      for (int b = 0; b < block_num; ++b)
      {
        try
        {
          // Use RecoveryToDatanode to notify datanode and push block content.
          // block id: here we don't have explicit id mapping in the request, use index b.
          // If your system needs specific block ids, adapt to include them in the request.
          RecoveryToDatanode(block_keys[b].c_str(), b, out_blocks[b], replacing_node_ip.c_str(), replacing_node_port);
          if (IF_DEBUG)
            std::cout << "[MultipleRecovery] sent recovered block " << block_keys[b] << " (index " << b << ") to " << replacing_node_ip << ":" << replacing_node_port << std::endl;
        }
        catch (const std::exception &e)
        {
          std::cerr << "[MultipleRecovery] exception while sending block " << b << " : " << e.what() << std::endl;
        }
      }

      // free temp buffers
      for (int i = 0; i < resource_proxy_num; ++i)
      {
        delete proxy_bufs[i];
      }
      delete[] proxy_bufs;

      for (int b = 0; b < block_num; ++b)
      {
        delete out_blocks[b];
      }
      delete[] out_blocks;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[MultipleRecovery] top-level exception: " << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }
  // delete
  grpc::Status ProxyImpl::deleteBlock(
      grpc::ServerContext *context,
      const proxy_proto::NodeAndBlock *node_and_block,
      proxy_proto::DelReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> nodes_ip_port;
    std::string key = node_and_block->key();
    int stripe_id = node_and_block->stripe_id();
    for (int i = 0; i < node_and_block->blockkeys_size(); i++)
    {
      blocks_id.push_back(node_and_block->blockkeys(i));
      std::string ip_port = node_and_block->datanodeip(i) + ":" + std::to_string(node_and_block->datanodeport(i));
      nodes_ip_port.push_back(ip_port);
    }
    auto delete_blocks = [this, key, blocks_id, stripe_id, nodes_ip_port]() mutable
    {
      auto request_and_delete = [this](std::string block_key, std::string node_ip_port)
      {
        bool ret = DelInDatanode(block_key, node_ip_port);
        if (!ret)
        {
          std::cout << "Delete value no return!" << std::endl;
          return;
        }
      };
      try
      {
        std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          senders.push_back(std::thread(request_and_delete, blocks_id[j], nodes_ip_port[j]));
        }

        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (stripe_id != -1 || key != "")
        {
          grpc::ClientContext c_context;
          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator rep;
          ECProject::OpperateType opp = DEL;
          commit_abort_key.set_opp(opp);
          commit_abort_key.set_key(key);
          commit_abort_key.set_ifcommitmetadata(true);
          commit_abort_key.set_stripe_id(stripe_id);
          grpc::Status stat;
          stat = m_coordinator_ptr->reportCommitAbort(&c_context, commit_abort_key, &rep);
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      std::thread my_thread(delete_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::getBlocks(grpc::ServerContext *context,
    const proxy_proto::StripeAndBlockIDs *request, proxy_proto::GetReply *response)
  {
    std::cout << "getting blocks" << "[" << request->block_ids(0) << "]" << "to" << "[" << request->block_ids(request->block_ids_size() - 1) << "]" << std::endl;
    int BlockSize = m_sys_config->BlockSize;
    size_t total_size = static_cast<size_t> (BlockSize) * request->block_ids_size();
    char *blocks = new char[total_size];
    uint32_t group_id = request->group_id();

    std::vector<std::thread> get_threads;
    for(int i = 0; i < request->block_ids_size(); i++)
    {
      get_threads.push_back(std::thread([this, i, &blocks, &request, BlockSize]() {
        this->GetFromDatanode(
            request->block_keys(i), 
            blocks + i * BlockSize,
            static_cast<size_t>(m_sys_config->BlockSize), 
            request->datanodeips(i).c_str(), 
            static_cast<int>(request->datanodeports(i))
        );    
      }));
      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket_data(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(request->clientip(), std::to_string(request->clientport()));;
      socket_data.connect(*endpoints, error);
      if (error)
      {
        std::cout << "error in connect" << std::endl;
      }
      std::cout << "connected to client" << std::endl;
      u_int32_t block_id = request->block_ids(i);
      asio::write(socket_data, asio::buffer(&block_id, sizeof(u_int32_t)));
      asio::write(socket_data, asio::buffer(blocks + i * static_cast<size_t>(BlockSize), BlockSize));
      asio::error_code ignore_ec;
      socket_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      socket_data.close(ignore_ec);
    }

    for(int i = 0; i < request->block_ids_size(); i++)
    {
      get_threads[i].join();
    }

    delete blocks;
    return grpc::Status();
  }

} // namespace ECProject