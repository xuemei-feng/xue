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
#include <cstdlib>
#include "unilrc_encoder.h"

namespace
{
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
}

int main(int argc, char **argv)
{
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string sys_config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../config/parameterConfiguration.xml";
    //std::string sys_config_path = "/home/GuanTian/lql/UniLRC/project/config/parameterConfiguration.xml";
    std::cout << "Current working directory: " << sys_config_path << std::endl;

    const ECProject::Config *config = ECProject::Config::getInstance(sys_config_path);
    std::string client_ip = "172.16.2.31";
    int client_port = 77777;
    ECProject::Client client(client_ip, client_port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path);
    std::cout << client.sayHelloToCoordinatorByGrpc("Client ID: " + client_ip + ":" + std::to_string(client_port)) << std::endl;

    std::vector<int> parameters = client.get_parameters();
    int k = parameters[0];
    int r = parameters[1];
    int z = parameters[2];
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
    if (argc >= 2)
    {
        std::ifstream trace_scan(argv[1]);
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
    std::cout << "Set phase: ClientStripeNum=" << stripe_num << ", total_write_size_mb=" << total_write_size << std::endl;
    const int set_threads = parse_cord_batch_threads();
    std::cout << "Set phase concurrency (CORD_BATCH_THREADS)=" << set_threads << std::endl;
    std::cout << "Starting set stripe operation" << std::endl;
    std::chrono::high_resolution_clock::time_point set_start = std::chrono::high_resolution_clock::now();
    if (set_threads <= 1)
    {
      for (int i = 0; i < stripe_num; i++)
      {
        client.set();
      }
    }
    else
    {
      // 每个线程独立 Client 实例，避免共享 m_pre_allocated_buffer
      std::vector<std::unique_ptr<ECProject::Client>> worker_clients;
      worker_clients.reserve(static_cast<size_t>(set_threads));
      for (int t = 0; t < set_threads; ++t)
      {
        const int port = client_port + 1 + t;
        worker_clients.push_back(std::make_unique<ECProject::Client>(
            client_ip, port, config->CoordinatorIP + ":" + std::to_string(config->CoordinatorPort), sys_config_path));
      }
      std::vector<std::thread> workers;
      std::atomic<int> next_idx{0};
      std::mutex set_log_mu;
      for (int t = 0; t < set_threads; ++t)
      {
        workers.emplace_back([&, t]() {
          ECProject::Client &wc = *worker_clients[static_cast<size_t>(t)];
          while (true)
          {
            int idx = next_idx.fetch_add(1);
            if (idx >= stripe_num)
              break;
            if (t == 0)
            {
              std::lock_guard<std::mutex> lk(set_log_mu);
              std::cout << "[set] stripe " << idx << " ..." << std::endl;
            }
            wc.set();
          }
        });
      }
      for (auto &th : workers)
        th.join();
    }
    std::chrono::high_resolution_clock::time_point set_end = std::chrono::high_resolution_clock::now();
    std::cout << "Set stripe operation finished" << std::endl;
    std::cout << "Conducting experiments, please wait..." << std::endl;
    std::chrono::duration<double> set_time = std::chrono::duration_cast<std::chrono::duration<double>>(set_end - set_start);
    std::cout << "write throughput: " << (static_cast<double> (total_write_size) / set_time.count() / 1024) << "MB/s" << std::endl;
    char input = 0;
    std::cout << "Start CoRD batch update? (type 'y' to proceed): " << std::endl;
    std::cin >> input;
    if (input == 'y')
    {
        if (argc < 2)
        {
            std::cout << "Usage: " << argv[0] << " <cord_update_trace_file>" << std::endl;
            std::cout << "Trace file: one request per line -> stripe_id range_count "
                         "then range_count pairs of (logical_offset_start logical_offset_end) "
                         "for half-open [start,end). Lines starting with # are ignored." << std::endl;
            return 1;
        }
        const std::string trace_path = argv[1];
        std::ifstream trace_file(trace_path);
        if (!trace_file.is_open())
        {
            std::cout << "Failed to open trace file: " << trace_path << std::endl;
            return 1;
        }

        const int batch_threads = parse_cord_batch_threads();
        if (batch_threads > 1)
        {
          std::cout << "[CoRD batch] CORD_BATCH_THREADS=" << batch_threads
                    << " ignored for CoRD trace (serial one-stripe-at-a-time mode)" << std::endl;
        }
        std::cout << "CoRD batch mode: serial (upload + xfer complete per stripe before next)" << std::endl;

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

        const double batch_wall_sec =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - batch_wall_t0).count();
        std::sort(batch_results.begin(), batch_results.end(),
                  [](const CordBatchResult &a, const CordBatchResult &b) { return a.line_no < b.line_no; });

        std::cout << "=== CoRD batch summary ===" << std::endl;
        std::cout << "trace_file=" << trace_path << std::endl;
        std::cout << "batch_mode=serial batch_wall_sec=" << std::fixed << std::setprecision(6)
                  << batch_wall_sec << std::endl;
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
    
    //for single block recovery
    /*
    std::cout << "Single block recovery test start" << std::endl;
    std::vector<std::chrono::duration<double>> block_recovery_time_spans;
    for(int i = 0; i < n; i++){
        std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
        client.recovery(0, i);
        std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        block_recovery_time_spans.push_back(time_span);
        //std::cout << "single block repair time: " << time_span.count() << std::endl;
    }
    std::chrono::duration<double> block_recovery_total_time_span = std::accumulate(block_recovery_time_spans.begin(), block_recovery_time_spans.end(), std::chrono::duration<double>(0));
    std::chrono::duration<double> block_recovery_max_time_span = *std::max_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    std::chrono::duration<double> block_recovery_min_time_span = *std::min_element(block_recovery_time_spans.begin(), block_recovery_time_spans.end());
    //std::cout << "Total time: " << total_time_span.count() << std::endl;
    std::cout << "Average time: " << block_recovery_total_time_span.count() / block_recovery_time_spans.size() << std::endl;
    std::cout << "Max time: "<< block_recovery_max_time_span.count() << std::endl;
    std::cout << "Min time: "<< block_recovery_min_time_span.count() << std::endl;
    std::cout << "Single block recovery test end" << std::endl;
    std::cout << std::endl;
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
