#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include "config.h"
#include <iomanip>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <memory>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <functional>
#include <cstdlib>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <climits>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numeric>
#include "unilrc_encoder.h"

namespace
{
  struct ClientRunOptions
  {
    std::string batch_file;
    std::string config_path;
    std::string client_ip;
    int client_port = 77777;
    bool initial_range_benchmark = false;
    bool dry_run = false;
    int threads = 0;
  };

  std::string default_config_path(const char *argv0)
  {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == nullptr)
      return "";
    std::string exe(argv0);
    const std::size_t slash = exe.rfind('/');
    if (slash == std::string::npos)
      return std::string(cwd) + "/../../config/parameterConfiguration.xml";
    return std::string(cwd) + exe.substr(1, slash) + "/../../config/parameterConfiguration.xml";
  }

  std::string ip_prefix(const std::string &ip)
  {
    const std::size_t last_dot = ip.rfind('.');
    if (last_dot == std::string::npos)
      return ip;
    return ip.substr(0, last_dot);
  }

  std::string detect_local_cluster_ip(const std::string &prefix)
  {
    struct ifaddrs *ifap = nullptr;
    if (getifaddrs(&ifap) != 0)
      return "";
    std::string found;
    for (struct ifaddrs *ifa = ifap; ifa != nullptr; ifa = ifa->ifa_next)
    {
      if (ifa->ifa_addr == nullptr || ifa->ifa_addr->sa_family != AF_INET)
        continue;
      char buf[INET_ADDRSTRLEN] = {};
      const auto *sin = reinterpret_cast<const struct sockaddr_in *>(ifa->ifa_addr);
      if (inet_ntop(AF_INET, &sin->sin_addr, buf, sizeof(buf)) == nullptr)
        continue;
      const std::string ip(buf);
      if (ip.rfind(prefix + ".", 0) == 0)
      {
        found = ip;
        break;
      }
    }
    freeifaddrs(ifap);
    return found;
  }

  std::string resolve_client_ip(const ECProject::Config *config)
  {
    if (const char *env = std::getenv("CORD_CLIENT_IP"))
    {
      if (env[0] != '\0')
        return env;
    }
    const std::string detected = detect_local_cluster_ip(ip_prefix(config->CoordinatorIP));
    if (!detected.empty())
      return detected;
    return "172.16.2.31";
  }

  bool parse_client_args(int argc, char **argv, ClientRunOptions &opts)
  {
    opts.config_path = default_config_path(argv[0]);
    for (int i = 1; i < argc; ++i)
    {
      const std::string arg = argv[i];
      if (arg == "--ip" && i + 1 < argc)
      {
        opts.client_ip = argv[++i];
      }
      else if (arg == "--port" && i + 1 < argc)
      {
        opts.client_port = std::atoi(argv[++i]);
      }
      else if (arg == "--config" && i + 1 < argc)
      {
        opts.config_path = argv[++i];
      }
      else if (arg == "--initial-range-benchmark")
      {
        opts.initial_range_benchmark = true;
      }
      else if (arg == "--dry-run")
      {
        opts.dry_run = true;
      }
      else if (arg == "--threads" && i + 1 < argc)
      {
        char *end = nullptr;
        const long value = std::strtol(argv[++i], &end, 10);
        if (end == argv[i] || *end != '\0' || value < 1 || value > 64)
        {
          std::cerr << "--threads must be an integer in [1,64]" << std::endl;
          return false;
        }
        opts.threads = static_cast<int>(value);
      }
      else if (arg == "--help" || arg == "-h")
      {
        return false;
      }
      else if (!arg.empty() && arg[0] != '-')
      {
        if (!opts.batch_file.empty())
        {
          std::cerr << "Multiple trace files specified" << std::endl;
          return false;
        }
        opts.batch_file = arg;
      }
      else
      {
        std::cerr << "Unknown argument: " << arg << std::endl;
        return false;
      }
    }
    if (opts.initial_range_benchmark && opts.batch_file.empty())
    {
      std::cerr << "--initial-range-benchmark requires a trace file" << std::endl;
      return false;
    }
    if (opts.dry_run && !opts.initial_range_benchmark)
    {
      std::cerr << "--dry-run requires --initial-range-benchmark" << std::endl;
      return false;
    }
    return true;
  }

  void print_client_usage(const char *argv0)
  {
    std::cout << "Usage: " << argv0
              << " [--ip CLIENT_IP] [--port PORT] [--config PATH]"
                 " [--initial-range-benchmark] [--dry-run] [--threads N] [trace_file]"
              << std::endl;
    std::cout << "Default mode keeps the existing RW/interactive CoRD flow. "
                 "Initial mode requires trace_file; --threads defaults to CORD_BATCH_THREADS or 1 (max 64)."
              << std::endl;
    std::cout << "Environment: CORD_CLIENT_IP overrides auto-detected cluster client IP." << std::endl;
  }

  bool parse_cord_trace_line(const std::string &line, int &stripe_id, int &range_cnt,
                             std::vector<std::pair<int, int>> &logical_ranges, std::string &err)
  {
    logical_ranges.clear();
    std::istringstream iss(line);
    if (!(iss >> stripe_id >> range_cnt))
    {
      err = "expected stripe_id range_count";
      return false;
    }
    if (range_cnt <= 0)
    {
      err = "range_count must be positive";
      return false;
    }
    logical_ranges.reserve(static_cast<size_t>(range_cnt));
    for (int i = 0; i < range_cnt; ++i)
    {
      int start = 0;
      int end = 0;
      if (!(iss >> start >> end))
      {
        err = "expected logical_offset_start logical_offset_end per range (half-open [start,end))";
        return false;
      }
      if (end <= start)
      {
        err = "invalid range [" + std::to_string(start) + "," + std::to_string(end) + ")";
        return false;
      }
      logical_ranges.emplace_back(start, end);
    }
    std::string extra;
    if (iss >> extra)
    {
      err = "trailing tokens after ranges";
      return false;
    }
    return true;
  }

  bool is_blank_or_comment_line(const std::string &line)
  {
    size_t i = 0;
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t' || line[i] == '\r'))
      ++i;
    if (i >= line.size())
      return true;
    return line[i] == '#';
  }

  std::ostream &print_cord_timing_fields(std::ostream &os, const ECProject::CordUpdateTiming &t)
  {
    os << "wall_sec=" << std::fixed << std::setprecision(6) << t.wall_sec
       << " plan_sec=" << t.plan_sec
       << " payload_prep_sec=" << t.payload_prep_sec
       << " upload_sec=" << t.upload_sec
       << " xfer_begin_sec=" << t.xfer_begin_sec
       << " xfer_wait_sec=" << t.xfer_wait_sec
       << " xfer_pure_sec=" << t.xfer_pure_sec
       << " xfer_grpc_sec=" << t.xfer_grpc_sec;
    return os;
  }

  struct CordTimingTotals
  {
    double wall_sec = 0.0;
    double plan_sec = 0.0;
    double payload_prep_sec = 0.0;
    double upload_sec = 0.0;
    double xfer_begin_sec = 0.0;
    double xfer_wait_sec = 0.0;
    double xfer_pure_sec = 0.0;
    double xfer_grpc_sec = 0.0;

    void add(const ECProject::CordUpdateTiming &t)
    {
      wall_sec += t.wall_sec;
      plan_sec += t.plan_sec;
      payload_prep_sec += t.payload_prep_sec;
      upload_sec += t.upload_sec;
      xfer_begin_sec += t.xfer_begin_sec;
      xfer_wait_sec += t.xfer_wait_sec;
      xfer_pure_sec += t.xfer_pure_sec;
      xfer_grpc_sec += t.xfer_grpc_sec;
    }

    ECProject::CordUpdateTiming avg(int count) const
    {
      ECProject::CordUpdateTiming out;
      if (count <= 0)
        return out;
      const double n = static_cast<double>(count);
      out.wall_sec = wall_sec / n;
      out.plan_sec = plan_sec / n;
      out.payload_prep_sec = payload_prep_sec / n;
      out.upload_sec = upload_sec / n;
      out.xfer_begin_sec = xfer_begin_sec / n;
      out.xfer_wait_sec = xfer_wait_sec / n;
      out.xfer_pure_sec = xfer_pure_sec / n;
      out.xfer_grpc_sec = xfer_grpc_sec / n;
      return out;
    }
  };

  struct CordBatchTask
  {
    int line_no = 0;
    int stripe_id = 0;
    std::vector<std::pair<int, int>> logical_ranges;
  };

  struct CordBatchResult
  {
    int line_no = 0;
    int stripe_id = 0;
    bool ok = false;
    ECProject::CordUpdateTiming timing;
  };

  int parse_cord_batch_threads()
  {
    const char *env = std::getenv("CORD_BATCH_THREADS");
    if (env == nullptr || env[0] == '\0')
      return 1;
    char *end = nullptr;
    long v = std::strtol(env, &end, 10);
    if (end == env || v < 1)
      return 1;
    if (v > 64)
      v = 64;
    return static_cast<int>(v);
  }

  bool parse_cord_pipeline_xfer()
  {
    const char *env = std::getenv("CORD_PIPELINE_XFER");
    if (env == nullptr || env[0] == '\0')
      return true;
    return !(env[0] == '0' && env[1] == '\0');
  }

  struct StripeXferGate
  {
    std::mutex mu;
    std::condition_variable cv;
    bool xfer_inflight = false;
  };

  void stripe_xfer_gate_wait_idle(StripeXferGate *gates, int gate_n, int stripe_id)
  {
    if (gates == nullptr || stripe_id < 0 || stripe_id >= gate_n)
      return;
    StripeXferGate &g = gates[stripe_id];
    std::unique_lock<std::mutex> lk(g.mu);
    g.cv.wait(lk, [&g] { return !g.xfer_inflight; });
  }

  void stripe_xfer_gate_mark_inflight(StripeXferGate *gates, int gate_n, int stripe_id)
  {
    if (gates == nullptr || stripe_id < 0 || stripe_id >= gate_n)
      return;
    StripeXferGate &g = gates[stripe_id];
    std::lock_guard<std::mutex> lk(g.mu);
    g.xfer_inflight = true;
  }

  void stripe_xfer_gate_clear_inflight(StripeXferGate *gates, int gate_n, int stripe_id)
  {
    if (gates == nullptr || stripe_id < 0 || stripe_id >= gate_n)
      return;
    StripeXferGate &g = gates[stripe_id];
    {
      std::lock_guard<std::mutex> lk(g.mu);
      g.xfer_inflight = false;
    }
    g.cv.notify_all();
  }

  struct InitialRangeTask
  {
    int line_no = 0;
    int stripe_id = 0;
    uint64_t logical_bytes = 0;
    std::vector<std::pair<uint64_t, uint64_t>> local_ranges;
  };

  struct InitialRangeResult
  {
    int line_no = 0;
    bool ok = false;
    ECProject::InitialRangeWriteStats stats;
  };

  double nearest_rank_percentile(std::vector<double> values, double percentile)
  {
    if (values.empty())
      return 0.0;
    std::sort(values.begin(), values.end());
    const size_t rank = static_cast<size_t>(std::ceil(percentile * static_cast<double>(values.size())));
    return values[std::max<size_t>(1, rank) - 1];
  }

  double nearest_rank_percentile_u64(std::vector<uint64_t> values, double percentile)
  {
    if (values.empty())
      return 0.0;
    std::sort(values.begin(), values.end());
    const size_t rank = static_cast<size_t>(std::ceil(percentile * static_cast<double>(values.size())));
    return static_cast<double>(values[std::max<size_t>(1, rank) - 1]);
  }

  bool parse_uint64_token(const std::string &token, uint64_t &value)
  {
    if (token.empty() || token[0] == '-')
      return false;
    char *end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(token.c_str(), &end, 10);
    if (errno == ERANGE || end == token.c_str() || *end != '\0')
      return false;
    value = static_cast<uint64_t>(parsed);
    return true;
  }

  bool load_initial_range_trace(const std::string &path, uint64_t stripe_data_bytes,
                                std::vector<InitialRangeTask> &tasks, int &max_stripe)
  {
    std::ifstream input(path);
    if (!input.is_open())
    {
      std::cerr << "Failed to open trace file: " << path << std::endl;
      return false;
    }
    std::unordered_set<int> stripe_ids;
    std::string line;
    int line_no = 0;
    max_stripe = -1;
    while (std::getline(input, line))
    {
      ++line_no;
      if (is_blank_or_comment_line(line))
        continue;
      std::istringstream iss(line);
      int64_t stripe_id_signed = -1;
      int64_t range_count_signed = -1;
      if (!(iss >> stripe_id_signed >> range_count_signed) || stripe_id_signed < 0 ||
          stripe_id_signed > std::numeric_limits<int>::max() || range_count_signed <= 0)
      {
        std::cerr << "Initial trace line " << line_no
                  << ": expected non-negative stripe_id and positive range_count" << std::endl;
        return false;
      }
      const int stripe_id = static_cast<int>(stripe_id_signed);
      if (!stripe_ids.insert(stripe_id).second)
      {
        std::cerr << "Initial trace line " << line_no << ": duplicate stripe_id=" << stripe_id << std::endl;
        return false;
      }
      if (stripe_data_bytes != 0 && static_cast<uint64_t>(stripe_id) >
                                        std::numeric_limits<uint64_t>::max() / stripe_data_bytes)
      {
        std::cerr << "Initial trace line " << line_no << ": stripe base offset overflow" << std::endl;
        return false;
      }
      const uint64_t stripe_base = static_cast<uint64_t>(stripe_id) * stripe_data_bytes;
      InitialRangeTask task;
      task.line_no = line_no;
      task.stripe_id = stripe_id;
      task.local_ranges.reserve(static_cast<size_t>(range_count_signed));
      for (int64_t i = 0; i < range_count_signed; ++i)
      {
        std::string start_token;
        std::string end_token;
        uint64_t global_start = 0;
        uint64_t global_end = 0;
        if (!(iss >> start_token >> end_token) ||
            !parse_uint64_token(start_token, global_start) ||
            !parse_uint64_token(end_token, global_end) || global_start >= global_end)
        {
          std::cerr << "Initial trace line " << line_no << ": invalid half-open range at index " << i << std::endl;
          return false;
        }
        if (global_start < stripe_base || global_end < stripe_base)
        {
          std::cerr << "Initial trace line " << line_no << ": range precedes stripe base" << std::endl;
          return false;
        }
        const uint64_t local_start = global_start - stripe_base;
        const uint64_t local_end = global_end - stripe_base;
        if (local_start > stripe_data_bytes || local_end > stripe_data_bytes || local_start >= local_end)
        {
          std::cerr << "Initial trace line " << line_no << ": range is outside stripe " << stripe_id << std::endl;
          return false;
        }
        task.local_ranges.emplace_back(local_start, local_end);
      }
      std::string extra;
      if (iss >> extra)
      {
        std::cerr << "Initial trace line " << line_no << ": trailing tokens" << std::endl;
        return false;
      }
      std::sort(task.local_ranges.begin(), task.local_ranges.end());
      std::vector<std::pair<uint64_t, uint64_t>> merged;
      for (const auto &range : task.local_ranges)
      {
        if (merged.empty() || range.first > merged.back().second)
          merged.push_back(range);
        else
          merged.back().second = std::max(merged.back().second, range.second);
      }
      for (const auto &range : merged)
      {
        const uint64_t length = range.second - range.first;
        if (task.logical_bytes > std::numeric_limits<uint64_t>::max() - length)
        {
          std::cerr << "Initial trace line " << line_no << ": logical byte count overflow" << std::endl;
          return false;
        }
        task.logical_bytes += length;
      }
      task.local_ranges = std::move(merged);
      max_stripe = std::max(max_stripe, stripe_id);
      tasks.push_back(std::move(task));
    }
    return true;
  }

  int run_initial_range_benchmark(const ClientRunOptions &opts, ECProject::Client &main_client,
                                  const ECProject::Config *config, const std::string &client_ip,
                                  int client_port, const std::string &config_path,
                                  int k, int block_size)
  {
    if (k <= 0 || block_size <= 0 ||
        static_cast<uint64_t>(block_size) > std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(k))
    {
      std::cerr << "Invalid k/BlockSize for initial range benchmark" << std::endl;
      return 1;
    }
    const uint64_t stripe_data_bytes = static_cast<uint64_t>(k) * static_cast<uint64_t>(block_size);
    std::vector<InitialRangeTask> tasks;
    int max_stripe = -1;
    if (!load_initial_range_trace(opts.batch_file, stripe_data_bytes, tasks, max_stripe))
      return 1;

    uint64_t trace_logical_bytes = 0;
    std::vector<uint64_t> request_sizes;
    request_sizes.reserve(tasks.size());
    for (const auto &task : tasks)
    {
      if (trace_logical_bytes > std::numeric_limits<uint64_t>::max() - task.logical_bytes)
      {
        std::cerr << "Trace logical byte count overflow" << std::endl;
        return 1;
      }
      trace_logical_bytes += task.logical_bytes;
      request_sizes.push_back(task.logical_bytes);
    }
    const double request_avg = request_sizes.empty() ? 0.0 :
        static_cast<double>(trace_logical_bytes) / static_cast<double>(request_sizes.size());
    std::cout << std::fixed << std::setprecision(6);
    std::cout << "initial_trace records=" << tasks.size()
              << " max_stripe=" << max_stripe
              << " logical_bytes=" << trace_logical_bytes
              << " logical_mib=" << static_cast<double>(trace_logical_bytes) / 1048576.0 << std::endl;
    std::cout << "request_size_bytes avg=" << request_avg
              << " p50=" << nearest_rank_percentile_u64(request_sizes, 0.50)
              << " p95=" << nearest_rank_percentile_u64(request_sizes, 0.95)
              << " p99=" << nearest_rank_percentile_u64(request_sizes, 0.99)
              << " max=" << (request_sizes.empty() ? 0 : *std::max_element(request_sizes.begin(), request_sizes.end()))
              << " percentile_method=nearest_rank" << std::endl;
    if (opts.dry_run)
      return 0;

    const int worker_count = opts.threads > 0 ? opts.threads : parse_cord_batch_threads();
    const std::string coordinator = config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort);
    std::vector<std::unique_ptr<ECProject::Client>> additional_clients;
    additional_clients.reserve(static_cast<size_t>(worker_count - 1));
    for (int worker_id = 1; worker_id < worker_count; ++worker_id)
      additional_clients.push_back(std::make_unique<ECProject::Client>(
          client_ip, client_port + worker_id, coordinator, config_path));

    std::atomic<size_t> next_task{0};
    std::atomic<size_t> completed{0};
    std::mutex result_mu;
    std::vector<InitialRangeResult> results;
    results.reserve(tasks.size());
    const auto batch_start = std::chrono::steady_clock::now();
    auto worker = [&](int worker_id) {
      ECProject::Client &client = worker_id == 0 ? main_client : *additional_clients[static_cast<size_t>(worker_id - 1)];
      for (;;)
      {
        const size_t index = next_task.fetch_add(1);
        if (index >= tasks.size())
          break;
        const InitialRangeTask &task = tasks[index];
        ECProject::InitialRangeWriteStats stats;
        const bool ok = client.initial_range_set(task.stripe_id, task.local_ranges, nullptr, 0, &stats);
        {
          std::lock_guard<std::mutex> lock(result_mu);
          results.push_back(InitialRangeResult{task.line_no, ok, stats});
        }
        const size_t done = completed.fetch_add(1) + 1;
        if (done % 1000 == 0 || done == tasks.size())
          std::cout << "initial_progress completed=" << done << " total=" << tasks.size() << std::endl;
      }
    };
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(worker_count));
    for (int worker_id = 0; worker_id < worker_count; ++worker_id)
      workers.emplace_back(worker, worker_id);
    for (auto &thread : workers)
      thread.join();
    const double batch_wall_sec = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - batch_start).count();

    size_t success = 0;
    uint64_t logical_bytes = 0, data_bytes = 0, parity_bytes = 0, upload_bytes = 0;
    double plan_total = 0.0, encode_total = 0.0, upload_total = 0.0;
    std::vector<double> latencies;
    for (const auto &result : results)
    {
      latencies.push_back(result.stats.wall_sec);
      if (!result.ok)
        continue;
      ++success;
      logical_bytes += result.stats.logical_bytes;
      data_bytes += result.stats.data_slice_bytes;
      parity_bytes += result.stats.parity_slice_bytes;
      upload_bytes += result.stats.upload_bytes;
      plan_total += result.stats.plan_sec;
      encode_total += result.stats.encode_sec;
      upload_total += result.stats.upload_sec;
    }
    const size_t failures = tasks.size() - success;
    const double logical_mib = static_cast<double>(logical_bytes) / 1048576.0;
    const double amplification = logical_bytes == 0 ? 0.0 :
        static_cast<double>(data_bytes + parity_bytes) / static_cast<double>(logical_bytes);
    const double latency_avg = latencies.empty() ? 0.0 :
        std::accumulate(latencies.begin(), latencies.end(), 0.0) / static_cast<double>(latencies.size());
    std::cout << "initial_summary requests=" << tasks.size() << " success=" << success
              << " failures=" << failures << " logical_bytes=" << logical_bytes
              << " logical_mib=" << logical_mib << " data_slice_bytes=" << data_bytes
              << " parity_slice_bytes=" << parity_bytes << " upload_bytes=" << upload_bytes
              << " write_amplification=" << amplification << " batch_wall_sec=" << batch_wall_sec
              << " logical_mib_s=" << (batch_wall_sec > 0.0 ? logical_mib / batch_wall_sec : 0.0)
              << " ops_s=" << (batch_wall_sec > 0.0 ? static_cast<double>(success) / batch_wall_sec : 0.0)
              << std::endl;
    std::cout << "latency_wall_sec avg=" << latency_avg
              << " p50=" << nearest_rank_percentile(latencies, 0.50)
              << " p95=" << nearest_rank_percentile(latencies, 0.95)
              << " p99=" << nearest_rank_percentile(latencies, 0.99)
              << " max=" << (latencies.empty() ? 0.0 : *std::max_element(latencies.begin(), latencies.end()))
              << " percentile_method=nearest_rank" << std::endl;
    std::cout << "stage_avg_sec plan=" << (success ? plan_total / success : 0.0)
              << " encode=" << (success ? encode_total / success : 0.0)
              << " upload=" << (success ? upload_total / success : 0.0) << std::endl;
    return failures == 0 ? 0 : 1;
  }

  struct CordPipelineSlot
  {
    ECProject::CordUpdatePending pending;
    CordBatchTask task;
  };
}

int main(int argc, char **argv)
{
    ClientRunOptions opts;
    if (!parse_client_args(argc, argv, opts))
    {
      print_client_usage(argv[0]);
      return 1;
    }

    const std::string sys_config_path = opts.config_path;
    if (!(opts.initial_range_benchmark && opts.dry_run))
      std::cout << "Config path: " << sys_config_path << std::endl;

    const ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
    std::string client_ip = opts.client_ip.empty() ? resolve_client_ip(config) : opts.client_ip;
    const int client_port = opts.client_port;
    if (!(opts.initial_range_benchmark && opts.dry_run))
      std::cout << "Client bind/advertise: " << client_ip << ":" << client_port << std::endl;
    ECProject::Client client(client_ip, client_port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path);
    if (opts.initial_range_benchmark && opts.dry_run)
      return run_initial_range_benchmark(opts, client, config, client_ip, client_port,
                                         sys_config_path, config->k, config->BlockSize);

    std::cout << client.sayHelloToCoordinatorByGrpc("Client ID: " + client_ip + ":" + std::to_string(client_port)) << std::endl;
    std::vector<int> parameters = client.get_parameters();
    if (parameters.size() < 5)
    {
      std::cerr << "Coordinator returned incomplete parameters" << std::endl;
      return 1;
    }
    int k = parameters[0];
    int r = parameters[1];
    int z = parameters[2];
    if (opts.initial_range_benchmark)
      return run_initial_range_benchmark(opts, client, config, client_ip, client_port,
                                         sys_config_path, k, parameters[3]);
    std::string code_type;
    if(parameters[4] == 0){
        code_type = "AzureLRC";
    }
    else if(parameters[4] == 1){
        code_type = "OptimalLRC";
    }
    else if(parameters[4] == 2){
        code_type = "UniformLRC";
    }
    else if(parameters[4] == 3){
        code_type = "UniLRC";
    }
    else{
        std::cout << "Code type error" << std::endl;
        return -1;
    }
    double block_size = static_cast<double> (parameters[3]) / 1024 / 1024; //MB
    int n = k + r + z;

    // 条带放置数量由 parameterConfiguration.xml 的 ClientStripeNum 控制
    const int stripe_num = config->ClientStripeNum;
    if (!opts.batch_file.empty())
    {
        std::ifstream trace_scan(opts.batch_file);
        int max_sid = -1;
        std::string scan_line;
        while (std::getline(trace_scan, scan_line))
        {
            if (scan_line.empty() || scan_line[0] == '#')
                continue;
            int sid = 0, rc = 0;
            std::istringstream siss(scan_line);
            if (siss >> sid >> rc)
                max_sid = std::max(max_sid, sid);
        }
        if (max_sid >= stripe_num)
        {
            std::cout << "[WARN] trace max stripe_id=" << max_sid
                      << " >= ClientStripeNum=" << stripe_num
                      << "; updates for stripe_id>=" << stripe_num << " will fail." << std::endl;
        }
    }
    const double total_write_size = static_cast<double>(stripe_num) * block_size * static_cast<double>(n); // MB
    const double stripe_read_mb = block_size * static_cast<double>(k); // MB per stripe (data only)
    const size_t expect_bytes = static_cast<size_t>(parameters[3]) * static_cast<size_t>(k);
    const int rw_trials = 10;
    const int set_threads = parse_cord_batch_threads();
    std::cout << "RW test: trials=" << rw_trials
              << ", ClientStripeNum=" << stripe_num
              << ", write_size_mb/trial=" << total_write_size
              << ", read_size_mb/stripe=" << stripe_read_mb
              << ", set_threads=" << set_threads << std::endl;

    // 多线程写时复用 worker Client，避免每轮重新绑端口
    std::vector<std::unique_ptr<ECProject::Client>> worker_clients;
    if (set_threads > 1)
    {
      worker_clients.reserve(static_cast<size_t>(set_threads));
      for (int t = 0; t < set_threads; ++t)
      {
        const int port = client_port + 1 + t;
        worker_clients.push_back(std::make_unique<ECProject::Client>(
            client_ip, port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path));
      }
    }

    std::vector<double> write_times;
    std::vector<double> write_throughputs;
    std::vector<double> read_times;
    std::vector<double> read_speeds;
    write_times.reserve(static_cast<size_t>(rw_trials));
    write_throughputs.reserve(static_cast<size_t>(rw_trials));
    read_times.reserve(static_cast<size_t>(rw_trials));
    read_speeds.reserve(static_cast<size_t>(rw_trials));

    int next_stripe_id = 0;
    for (int trial = 1; trial <= rw_trials; ++trial)
    {
      std::cout << "========== RW trial " << trial << "/" << rw_trials
                << " (stripes " << next_stripe_id << ".." << (next_stripe_id + stripe_num - 1) << ") ==========" << std::endl;

      // ---- write ----
      std::cout << "[write] trial " << trial << " start" << std::endl;
      std::chrono::high_resolution_clock::time_point set_start = std::chrono::high_resolution_clock::now();
      if (set_threads <= 1)
      {
        for (int i = 0; i < stripe_num; i++)
          client.set();
      }
      else
      {
        std::vector<std::thread> workers;
        std::atomic<int> next_idx{0};
        for (int t = 0; t < set_threads; ++t)
        {
          workers.emplace_back([&, t]() {
            ECProject::Client &wc = *worker_clients[static_cast<size_t>(t)];
            while (true)
            {
              int idx = next_idx.fetch_add(1);
              if (idx >= stripe_num)
                break;
              wc.set();
            }
          });
        }
        for (auto &th : workers)
          th.join();
      }
      std::chrono::high_resolution_clock::time_point set_end = std::chrono::high_resolution_clock::now();
      const double set_sec =
          std::chrono::duration_cast<std::chrono::duration<double>>(set_end - set_start).count();
      const double write_mbs = total_write_size / set_sec;
      write_times.push_back(set_sec);
      write_throughputs.push_back(write_mbs);
      std::cout << "[write] trial " << trial << " time=" << set_sec
                << " s, throughput=" << write_mbs << " MB/s" << std::endl;

      // ---- read（读本轮刚写入的条带）----
      std::cout << "[read] trial " << trial << " start" << std::endl;
      std::vector<std::chrono::duration<double>> read_time_spans;
      read_time_spans.reserve(static_cast<size_t>(stripe_num));
      int read_ok = 0;
      int verify_ok = 0;
      for (int i = 0; i < stripe_num; i++)
      {
        const int sid = next_stripe_id + i;
        size_t data_size = 0;
        const std::string key = std::to_string(sid);
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        std::shared_ptr<char[]> data = client.get(key, data_size);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span =
            std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        if (!data || data_size != expect_bytes)
        {
          std::cout << "[read] trial " << trial << " stripe " << sid << " failed"
                    << (data ? (" size=" + std::to_string(data_size)) : "") << std::endl;
          continue;
        }
        read_time_spans.push_back(time_span);
        ++read_ok;
        bool payload_ok = true;
        const unsigned char *p = reinterpret_cast<const unsigned char *>(data.get());
        for (size_t b = 0; b < data_size; ++b)
        {
          if (p[b] != 0xaa)
          {
            payload_ok = false;
            break;
          }
        }
        if (payload_ok)
          ++verify_ok;
        else
          std::cout << "[read] trial " << trial << " stripe " << sid
                    << " payload mismatch (expect 0xaa)" << std::endl;
        std::cout << "[read] trial " << trial << " stripe " << sid
                  << " time=" << time_span.count() << " s" << std::endl;
      }
      std::cout << "[read] trial " << trial << " success: " << read_ok << "/" << stripe_num
                << ", verify ok: " << verify_ok << "/" << stripe_num << std::endl;
      if (!read_time_spans.empty())
      {
        const std::chrono::duration<double> read_total =
            std::accumulate(read_time_spans.begin(), read_time_spans.end(),
                            std::chrono::duration<double>(0));
        const double avg_sec = read_total.count() / static_cast<double>(read_time_spans.size());
        const double read_mbs = stripe_read_mb / avg_sec;
        read_times.push_back(avg_sec);
        read_speeds.push_back(read_mbs);
        std::cout << "[read] trial " << trial << " avg_time=" << avg_sec
                  << " s, avg_speed=" << read_mbs << " MB/s" << std::endl;
      }
      else
      {
        std::cout << "[read] trial " << trial << " no successful reads" << std::endl;
      }
      std::cout << std::endl;
      next_stripe_id += stripe_num;
    }

    auto print_stats = [](const char *name, const std::vector<double> &vals) {
      if (vals.empty())
      {
        std::cout << name << ": no samples" << std::endl;
        return;
      }
      const double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
      const double avg = sum / static_cast<double>(vals.size());
      const double mx = *std::max_element(vals.begin(), vals.end());
      const double mn = *std::min_element(vals.begin(), vals.end());
      std::cout << name << " per-trial:";
      for (size_t i = 0; i < vals.size(); ++i)
        std::cout << " [" << (i + 1) << "]=" << vals[i];
      std::cout << std::endl;
      std::cout << name << " avg=" << avg << ", max=" << mx << ", min=" << mn << std::endl;
    };
    std::cout << "========== RW summary (" << rw_trials << " trials) ==========" << std::endl;
    print_stats("write time(s)", write_times);
    print_stats("write throughput(MB/s)", write_throughputs);
    print_stats("read avg time(s)", read_times);
    print_stats("read avg speed(MB/s)", read_speeds);
    std::cout << std::endl;

    char input = 0;
    std::cout << "Start CoRD batch update? (type 'y' to proceed, other to skip): " << std::endl;
    std::cin >> input;
    if (input == 'y')
    {
        if (opts.batch_file.empty())
        {
            print_client_usage(argv[0]);
            std::cout << "Trace file: one request per line -> stripe_id range_count "
                         "then range_count pairs of (logical_offset_start logical_offset_end) "
                         "for half-open [start,end). Lines starting with # are ignored." << std::endl;
            return 1;
        }
        const std::string trace_path = opts.batch_file;
        std::ifstream trace_file(trace_path);
        if (!trace_file.is_open())
        {
            std::cout << "Failed to open trace file: " << trace_path << std::endl;
            return 1;
        }

        const int batch_threads = parse_cord_batch_threads();
        const std::string coord_addr = config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort);
        std::cout << "CoRD batch parallel workers (CORD_BATCH_THREADS)=" << batch_threads << std::endl;

        std::vector<CordBatchTask> tasks;
        tasks.reserve(256);
        std::string line;
        int line_no = 0;
        int parse_failures = 0;
        while (std::getline(trace_file, line))
        {
            ++line_no;
            if (is_blank_or_comment_line(line))
                continue;
            CordBatchTask task;
            task.line_no = line_no;
            int range_cnt = 0;
            std::string parse_err;
            if (!parse_cord_trace_line(line, task.stripe_id, range_cnt, task.logical_ranges, parse_err))
            {
                std::cout << "[CoRD batch] line " << line_no << " parse error: " << parse_err
                          << " (skipped, continue)" << std::endl;
                ++parse_failures;
                continue;
            }
            tasks.push_back(std::move(task));
        }

        int total_failures = parse_failures;
        int success_count = 0;
        CordTimingTotals timing_totals;
        std::vector<CordBatchResult> batch_results;
        batch_results.reserve(tasks.size());

        const auto batch_wall_t0 = std::chrono::steady_clock::now();
        const bool pipeline_xfer = parse_cord_pipeline_xfer();
        const int stripe_gate_n = std::max(stripe_num, 1);
        std::vector<StripeXferGate> stripe_xfer_gates(static_cast<size_t>(stripe_gate_n));
        std::cout << "CORD_PIPELINE_XFER=" << (pipeline_xfer ? 1 : 0)
                  << " (defer xfer wait; overlap upload with prior stripe xfer)" << std::endl;

        std::mutex log_mu;
        std::mutex result_mu;

        auto record_batch_result = [&](const CordBatchTask &task, bool ok, const ECProject::CordUpdateTiming &timing,
                                       int worker_id) {
            if (worker_id >= 0)
            {
                std::lock_guard<std::mutex> lk(log_mu);
                std::cout << "[CoRD batch][w" << worker_id << "] line " << task.line_no
                          << (ok ? " OK " : " FAILED ");
                print_cord_timing_fields(std::cout, timing);
                if (!ok)
                    std::cout << std::endl;
                else
                    std::cout << std::endl;
            }
            else
            {
                std::cout << "[CoRD batch] line " << task.line_no << (ok ? " OK " : " FAILED ");
                print_cord_timing_fields(std::cout, timing);
                std::cout << (ok ? "" : " (skipped, continue)") << std::endl;
            }
            std::lock_guard<std::mutex> rlk(result_mu);
            batch_results.push_back(CordBatchResult{task.line_no, task.stripe_id, ok, timing});
            if (ok)
            {
                ++success_count;
                timing_totals.add(timing);
            }
            else
                ++total_failures;
        };

        auto finalize_pipeline_stripe = [&](ECProject::Client &wc, int worker_id,
                                            std::unordered_map<int, CordPipelineSlot> &slots, int stripe_id) {
            auto sit = slots.find(stripe_id);
            if (sit == slots.end())
                return;
            ECProject::CordUpdateTiming timing;
            const bool ok = wc.cord_update_wait_xfer(&sit->second.pending, &timing);
            stripe_xfer_gate_clear_inflight(stripe_xfer_gates.data(), stripe_gate_n, stripe_id);
            const CordBatchTask task = sit->second.task;
            slots.erase(sit);
            record_batch_result(task, ok, timing, worker_id);
        };

        auto drain_pipeline_slots = [&](ECProject::Client &wc, int worker_id,
                                        std::unordered_map<int, CordPipelineSlot> &slots) {
            while (!slots.empty())
            {
                const int sid = slots.begin()->first;
                finalize_pipeline_stripe(wc, worker_id, slots, sid);
            }
        };

        auto run_pipeline_task = [&](ECProject::Client &wc, int worker_id, const CordBatchTask &task,
                                     std::unordered_map<int, CordPipelineSlot> &slots) {
            if (task.stripe_id < 0 || task.stripe_id >= stripe_gate_n)
            {
                ECProject::CordUpdateTiming timing;
                if (worker_id >= 0)
                {
                    std::lock_guard<std::mutex> lk(log_mu);
                    std::cout << "[CoRD batch][w" << worker_id << "] line " << task.line_no
                              << " stripe_id=" << task.stripe_id << " out of range" << std::endl;
                }
                else
                {
                    std::cout << "[CoRD batch] line " << task.line_no << " stripe_id=" << task.stripe_id
                              << " out of range" << std::endl;
                }
                std::lock_guard<std::mutex> rlk(result_mu);
                ++total_failures;
                batch_results.push_back(CordBatchResult{task.line_no, task.stripe_id, false, timing});
                return;
            }

            finalize_pipeline_stripe(wc, worker_id, slots, task.stripe_id);
            stripe_xfer_gate_wait_idle(stripe_xfer_gates.data(), stripe_gate_n, task.stripe_id);

            if (worker_id >= 0)
            {
                std::lock_guard<std::mutex> lk(log_mu);
                std::cout << "[CoRD batch][w" << worker_id << "] line " << task.line_no
                          << " stripe_id=" << task.stripe_id
                          << " ranges=" << task.logical_ranges.size() << " ..." << std::endl;
            }
            else
            {
                std::cout << "[CoRD batch] line " << task.line_no << " stripe_id=" << task.stripe_id
                          << " ranges=" << task.logical_ranges.size() << " ..." << std::endl;
            }

            ECProject::CordUpdatePending pending;
            ECProject::CordUpdateTiming partial;
            const bool started =
                wc.cord_update_start(task.stripe_id, task.logical_ranges, nullptr, 0, &pending, &partial);
            if (!started)
            {
                record_batch_result(task, false, partial, worker_id);
                return;
            }
            if (pending.transfer_plan_key.empty())
            {
                record_batch_result(task, true, partial, worker_id);
                return;
            }
            stripe_xfer_gate_mark_inflight(stripe_xfer_gates.data(), stripe_gate_n, task.stripe_id);
            CordPipelineSlot slot;
            slot.pending = std::move(pending);
            slot.task = task;
            slots[task.stripe_id] = std::move(slot);
        };

        if (batch_threads <= 1 && !pipeline_xfer)
        {
            for (const CordBatchTask &task : tasks)
            {
                std::cout << "[CoRD batch] line " << task.line_no << " stripe_id=" << task.stripe_id
                          << " ranges=" << task.logical_ranges.size() << " ..." << std::endl;
                ECProject::CordUpdateTiming timing;
                const bool ok = client.cord_update(task.stripe_id, task.logical_ranges, nullptr, 0, &timing);
                batch_results.push_back(CordBatchResult{task.line_no, task.stripe_id, ok, timing});
                if (!ok)
                {
                    std::cout << "[CoRD batch] line " << task.line_no << " FAILED ";
                    print_cord_timing_fields(std::cout, timing);
                    std::cout << " (skipped, continue)" << std::endl;
                    ++total_failures;
                    continue;
                }
                ++success_count;
                timing_totals.add(timing);
                std::cout << "[CoRD batch] line " << task.line_no << " OK ";
                print_cord_timing_fields(std::cout, timing);
                std::cout << std::endl;
            }
        }
        else if (batch_threads <= 1)
        {
            std::unordered_map<int, CordPipelineSlot> pipeline_slots;
            for (const CordBatchTask &task : tasks)
                run_pipeline_task(client, -1, task, pipeline_slots);
            drain_pipeline_slots(client, -1, pipeline_slots);
        }
        else if (!pipeline_xfer)
        {
            std::vector<std::unique_ptr<ECProject::Client>> worker_clients;
            worker_clients.reserve(static_cast<size_t>(batch_threads));
            for (int wi = 0; wi < batch_threads; ++wi)
            {
                const int port = client_port + 1 + wi;
                worker_clients.push_back(std::make_unique<ECProject::Client>(
                    client_ip, port, coord_addr, sys_config_path));
                std::cout << "[CoRD batch] worker " << wi << " client_id=" << client_ip << ":" << port << std::endl;
            }

            const int stripe_lock_n = std::max(stripe_num, 1);
            std::vector<std::mutex> stripe_locks(static_cast<size_t>(stripe_lock_n));
            std::atomic<size_t> next_task{0};

            auto worker_fn = [&](int worker_id) {
                ECProject::Client &wc = *worker_clients[static_cast<size_t>(worker_id)];
                for (;;)
                {
                    const size_t idx = next_task.fetch_add(1);
                    if (idx >= tasks.size())
                        break;
                    const CordBatchTask &task = tasks[idx];
                    if (task.stripe_id < 0 || task.stripe_id >= stripe_lock_n)
                    {
                        ECProject::CordUpdateTiming timing;
                        std::lock_guard<std::mutex> lk(log_mu);
                        std::cout << "[CoRD batch][w" << worker_id << "] line " << task.line_no
                                  << " stripe_id=" << task.stripe_id << " out of range" << std::endl;
                        std::lock_guard<std::mutex> rlk(result_mu);
                        ++total_failures;
                        batch_results.push_back(CordBatchResult{task.line_no, task.stripe_id, false, timing});
                        continue;
                    }
                    {
                        std::lock_guard<std::mutex> lk(log_mu);
                        std::cout << "[CoRD batch][w" << worker_id << "] line " << task.line_no
                                  << " stripe_id=" << task.stripe_id
                                  << " ranges=" << task.logical_ranges.size() << " ..." << std::endl;
                    }
                    ECProject::CordUpdateTiming timing;
                    bool ok = false;
                    {
                        std::lock_guard<std::mutex> stripe_lk(stripe_locks[static_cast<size_t>(task.stripe_id)]);
                        ok = wc.cord_update(task.stripe_id, task.logical_ranges, nullptr, 0, &timing);
                    }
                    record_batch_result(task, ok, timing, worker_id);
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(static_cast<size_t>(batch_threads));
            for (int wi = 0; wi < batch_threads; ++wi)
                threads.emplace_back(worker_fn, wi);
            for (auto &th : threads)
                th.join();
        }
        else
        {
            std::vector<std::unique_ptr<ECProject::Client>> worker_clients;
            worker_clients.reserve(static_cast<size_t>(batch_threads));
            for (int wi = 0; wi < batch_threads; ++wi)
            {
                const int port = client_port + 1 + wi;
                worker_clients.push_back(std::make_unique<ECProject::Client>(
                    client_ip, port, coord_addr, sys_config_path));
                std::cout << "[CoRD batch] worker " << wi << " client_id=" << client_ip << ":" << port << std::endl;
            }

            std::atomic<size_t> next_task{0};

            auto worker_fn = [&](int worker_id) {
                ECProject::Client &wc = *worker_clients[static_cast<size_t>(worker_id)];
                std::unordered_map<int, CordPipelineSlot> pipeline_slots;
                for (;;)
                {
                    const size_t idx = next_task.fetch_add(1);
                    if (idx >= tasks.size())
                        break;
                    run_pipeline_task(wc, worker_id, tasks[idx], pipeline_slots);
                }
                drain_pipeline_slots(wc, worker_id, pipeline_slots);
            };

            std::vector<std::thread> threads;
            threads.reserve(static_cast<size_t>(batch_threads));
            for (int wi = 0; wi < batch_threads; ++wi)
                threads.emplace_back(worker_fn, wi);
            for (auto &th : threads)
                th.join();
        }

        const double batch_wall_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - batch_wall_t0).count();
        std::sort(batch_results.begin(), batch_results.end(),
                  [](const CordBatchResult &a, const CordBatchResult &b) { return a.line_no < b.line_no; });

        std::cout << "=== CoRD batch summary ===" << std::endl;
        std::cout << "trace_file=" << trace_path << std::endl;
        std::cout << "batch_threads=" << batch_threads << std::endl;
        for (const CordBatchResult &br : batch_results)
        {
            if (!br.ok)
                continue;
            std::cout << "  success[line=" << br.line_no << " stripe=" << br.stripe_id << "] ";
            print_cord_timing_fields(std::cout, br.timing);
            std::cout << std::endl;
        }
        std::cout << "success_count=" << success_count << std::endl;
        std::cout << "total_failures=" << total_failures << std::endl;
        std::cout << "batch_total ";
        print_cord_timing_fields(std::cout, ECProject::CordUpdateTiming{
            timing_totals.wall_sec, timing_totals.plan_sec, timing_totals.payload_prep_sec,
            timing_totals.upload_sec, timing_totals.xfer_begin_sec, timing_totals.xfer_wait_sec,
            timing_totals.xfer_pure_sec, timing_totals.xfer_grpc_sec});
        std::cout << std::endl;
        if (success_count > 0)
        {
            const ECProject::CordUpdateTiming avg_timing = timing_totals.avg(success_count);
            std::cout << "batch_avg ";
            print_cord_timing_fields(std::cout, avg_timing);
            std::cout << std::endl;
        }
        std::cout << "[CoRD] e2e_wall_sec=" << std::fixed << std::setprecision(6) << batch_wall_sec
                  << " success=" << success_count << " failures=" << total_failures << std::endl;
        if (total_failures > 0)
            return 1;
    }
    else
    {
        std::cout << "Update cancelled." << std::endl;
    }

    
    // std::string output_file_name = "test_"  + code_type + + "_" + std::to_string(k) + "_" + std::to_string(r) + "_" + std::to_string(z) + ".txt";
    // std::ofstream output_file(output_file_name);
    // if (!output_file.is_open())
    // {
    //     std::cerr << "Error opening file: " << output_file_name << std::endl;
    //     return 1;
    // }
    // freopen(output_file_name.c_str(), "w", stdout);
    // std::mt19937 rng(std::random_device{}());

    // std::uniform_int_distribution<int> dist_500(0, k*stripe_num - 500);
    // std::uniform_real_distribution<double> dist_double(0.0, 1.0);
    
    /*std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ibm_test_trace.csv";
    std::fstream trace_file(trace_file_path);
    std::string trace_line;
    while(std::getline(trace_file, trace_line)){
        std::string operation;
        int operation_size;
        std::istringstream iss(trace_line);
        std::getline(iss, operation, ',');
        iss >> operation_size;
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        if(operation == "GET"){
            int start_block_id = dist_500(rng);
            client.get_blocks(start_block_id, start_block_id + operation_size - 1);
        }
        else if(operation == "PUT"){
            client.sub_set(operation_size);
        }
        else{
            std::cerr << "Unknown operation: " << operation << std::endl;
            return -1;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    }*/

    /*std::string trace_file_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../trace/ycsb_final.txt";
    std::fstream trace_file(trace_file_path);
    std::string trace_line;
    while(std::getline(trace_file, trace_line)){
        std::string operation;
        std::istringstream iss(trace_line);
        std::getline(iss, operation, ' ');
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        if(operation == "R"){
            int block_id;
            iss >> block_id;
            client.get_blocks(block_id, block_id);
        }
        else if(operation == "U"){
            client.sub_set(1);
        }
        else{
            std::cerr << "Unknown operation: " << operation << std::endl;
            return -1;
        }
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << operation << " operation time: " << time_span.count() << " seconds" << std::endl;
    }*/

    
    //for read test
    // std::cout << "Normal read test start" << std::endl;
    // std::vector<std::chrono::duration<double>> read_time_spans;
    // for(int i = 0; i < 5; i++){
    //     size_t data_size;
    //     int id = i;
    //     std::string key = std::to_string(id);
    //     std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    //     std::shared_ptr<char[]> data = client.get(key, data_size);
    //     if(!data){
    //         std::cout << "Get operation failed" << std::endl;
    //         continue;
    //     }
    //     std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //     read_time_spans.push_back(time_span);
    //     //std::cout << "get time: " << time_span.count() << std::endl;
    // }
    // std::chrono::duration<double> read_total_time_span = std::accumulate(read_time_spans.begin(), read_time_spans.end(), std::chrono::duration<double>(0));
    // std::cout << "Total time: " << read_total_time_span.count() << std::endl;
    // std::cout << "Average time: " << read_total_time_span.count() / read_time_spans.size() << std::endl;
    // std::cout << "Throughput: " << read_time_spans.size() / read_total_time_span.count() << std::endl;
    // std::cout << "Speed" << static_cast<size_t>(block_size) * k / (read_total_time_span.count() / read_time_spans.size()) << "MB/s" << std::endl;
    // std::chrono::duration<double> read_max_time_span = *std::max_element(read_time_spans.begin(), read_time_spans.end());
    // std::chrono::duration<double> read_min_time_span = *std::min_element(read_time_spans.begin(), read_time_spans.end());
    // std::cout << "Max speed: " << static_cast<size_t>(block_size) * k / read_min_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Min speed: " << static_cast<size_t>(block_size) * k / read_max_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Normal read test end" << std::endl;
    // std::cout << std::endl;
    
    // //for degraded read test
    // std::vector<std::chrono::duration<double>> degraded_read_time_spans;
    // std::cout << "Degraded read test start" << std::endl;
    // for(int i = 0; i < k; i++){
    //     size_t data_size;
    //     int id = i;
    //     std::string key = std::to_string(id);
    //     std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    //     std::shared_ptr<char[]> data = client.get_degraded_read_block(0, i);
    //     if(!data){
    //         std::cout << "Degraded read operation failed" << std::endl;
    //         continue;
    //     }
    //     std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    //     std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    //     degraded_read_time_spans.push_back(time_span);
    //     //std::cout << "get time: " << time_span.count() << std::endl;
    // }
    // std::chrono::duration<double> degraded_read_total_time_span = std::accumulate(degraded_read_time_spans.begin(), degraded_read_time_spans.end(), std::chrono::duration<double>(0));
    // std::cout << "Average time: " << degraded_read_total_time_span.count() / degraded_read_time_spans.size() << std::endl;
    // std::chrono::duration<double> degraded_read_max_time_span = *std::max_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    // std::chrono::duration<double> degraded_read_min_time_span = *std::min_element(degraded_read_time_spans.begin(), degraded_read_time_spans.end());
    // std::cout << "Max time: "<< degraded_read_max_time_span.count() << std::endl;
    // std::cout << "Min time: "<< degraded_read_min_time_span.count() << std::endl;
    // std::cout << "Throughput: " << degraded_read_time_spans.size() / degraded_read_total_time_span.count() << std::endl;
    // std::cout << "Speed" << static_cast<size_t>(block_size)  / (degraded_read_total_time_span.count() / degraded_read_time_spans.size()) << "MB/s" << std::endl;
    // std::cout << "Max speed: " << static_cast<size_t>(block_size)  / degraded_read_min_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Min speed: " << static_cast<size_t>(block_size)  / degraded_read_max_time_span.count() << "MB/s" << std::endl;
    // std::cout << "Degraded read test end" << std::endl;
    // std::cout << std::endl;
    

    /*
    // single block recovery: repair all n blocks of stripe 0 and report average / max / min
    {
        std::cout << "Single block recovery test start (stripe 0, blocks 0.." << (n - 1) << ")" << std::endl;
        std::vector<std::chrono::duration<double>> block_recovery_time_spans;
        int success_cnt = 0;
        for (int i = 0; i < n; i++)
        {
            std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
            bool ok = client.recovery(0, i);
            std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
            block_recovery_time_spans.push_back(time_span);
            if (ok)
                success_cnt++;
            else
                std::cout << "Single block recovery failed for block " << i << std::endl;
            std::cout << "block " << i << " recovery time: " << time_span.count() << " seconds"
                      << (ok ? "" : " (failed)") << std::endl;
        }
        std::chrono::duration<double> block_recovery_total_time_span = std::accumulate(
            block_recovery_time_spans.begin(), block_recovery_time_spans.end(), std::chrono::duration<double>(0));
        std::chrono::duration<double> block_recovery_max_time_span = *std::max_element(
            block_recovery_time_spans.begin(), block_recovery_time_spans.end());
        std::chrono::duration<double> block_recovery_min_time_span = *std::min_element(
            block_recovery_time_spans.begin(), block_recovery_time_spans.end());
        std::cout << "Success: " << success_cnt << "/" << n << std::endl;
        std::cout << "Average time: " << block_recovery_total_time_span.count() / block_recovery_time_spans.size() << std::endl;
        std::cout << "Max time: " << block_recovery_max_time_span.count() << std::endl;
        std::cout << "Min time: " << block_recovery_min_time_span.count() << std::endl;
        std::cout << "Single block recovery test end" << std::endl;
        std::cout << std::endl;
    }
  */
    
    /*
    //for full node repair
    std::cout << "Full node repair test start" << std::endl;
    int node_num = 5;
    std::vector<int> node_ids;
    while(node_ids.size() < node_num){
        int random_id = rand() % (19 * 30);
        if(std::find(node_ids.begin(), node_ids.end(), random_id) == node_ids.end()){
            node_ids.push_back(random_id);
        }
    }

    std::vector<double> full_node_recovery_speeds;
    for(int i = 0; i < node_num; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        int block_num = client.recovery_full_node(node_ids[i]);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        //std::cout << "full node repair time: " << time_span.count() << std::endl;
        //std::cout << "block num: " << block_num << std::endl;
        double total_size = block_num * block_size; //MB
        //std::cout << "Speed: " << total_size / time_span.count() << "MB/s" << std::endl;
        full_node_recovery_speeds.push_back(total_size / time_span.count());
    }
    std::cout << "Average speed: " << std::accumulate(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end(), 0.0) / full_node_recovery_speeds.size() << "MB/s" << std::endl;
    std::cout << "Max speed: " << *std::max_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Min speed: " << *std::min_element(full_node_recovery_speeds.begin(), full_node_recovery_speeds.end()) << "MB/s" << std::endl;
    std::cout << "Full node repair test end" << std::endl;
    std::cout << std::endl;
    //for decode test
    std::cout << "Decode test start" << std::endl;
    std::vector<double> decode_time_spans;
    for(int i = 0; i < n; i++){
        double decode_time_span;
        client.decode_test(0, i, client_ip, client_port, decode_time_span);
        decode_time_spans.push_back(decode_time_span);
    }
    std::cout << "Average decode time: " << std::endl;
    std::cout << std::accumulate(decode_time_spans.begin(), decode_time_spans.end(), 0.0) / decode_time_spans.size() << std::endl;
    std::cout << "Decode test end" << std::endl;
    std::cout << std::endl;*/
    return 0;
}
