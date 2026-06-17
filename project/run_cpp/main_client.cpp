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
#include "unilrc_encoder.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <map>
#include <future>

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
    std::string client_ip = "10.10.1.1";
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

    // 固定预填充条带数；否则块变小时间接写满「3000MB」会导致条带数暴涨
    const int stripe_num = 1000;
    const double total_write_size = static_cast<double>(stripe_num) * block_size * static_cast<double>(n); // MB
    std::cout << "Set phase: stripe_num=" << stripe_num << ", total_write_size_mb=" << total_write_size << std::endl;
    std::cout << "Starting set stripe operation" << std::endl;
    std::chrono::high_resolution_clock::time_point set_start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < stripe_num; i++){
        client.set();
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

        // ===== 并行 batch 处理开始 =====
        // 可配置的 stripe 级并发度（默认 4）
        constexpr int MAX_CONCURRENT_STRIPES = 16;

        int total_failures = 0;
        int success_count = 0;
        double success_wall_sum = 0.0;  // 各请求独立耗时累加（用于 avg_wall_sec_per_request）
        std::vector<double> per_success_wall_sec;
        per_success_wall_sec.reserve(64);

        // 用于计算 batch 真实墙钟时间（从第一个请求开始到最后一个请求结束）
        auto batch_wall_start = std::chrono::steady_clock::now();
        bool batch_has_any = false;
        auto batch_wall_end = batch_wall_start;

        std::string line;
        int line_no = 0;

        // per-stripe 互斥锁：同一 stripe 的请求必须串行
        std::map<int, std::unique_ptr<std::mutex>> stripe_mutex_map;
        // 全局并发计数 + 条件变量
        std::mutex concurrency_mu;
        std::condition_variable concurrency_cv;
        int active_stripes = 0;

        // 存储异步任务结果
        struct RequestResult {
            int line_no;
            int stripe_id;
            double wall_sec;
            bool success;
        };
        std::vector<std::future<RequestResult>> futures;

        auto process_one_line = [&](const std::string& ln, int lno) -> RequestResult {
            RequestResult res{lno, 0, 0.0, false};
            int stripe_id = 0;
            int range_cnt = 0;
            std::vector<std::pair<int, int>> logical_ranges;
            std::string parse_err;
            if (!parse_cord_trace_line(ln, stripe_id, range_cnt, logical_ranges, parse_err)) {
                std::cout << "[CoRD batch] line " << lno << " parse error: " << parse_err << " (skipped)" << std::endl;
                res.success = false;
                return res;
            }
            res.stripe_id = stripe_id;

            // 获取该 stripe 的专用锁
            std::unique_lock<std::mutex> stripe_lk;
            {
                std::lock_guard<std::mutex> map_lk(concurrency_mu);
                if (stripe_mutex_map.find(stripe_id) == stripe_mutex_map.end()) {
                    stripe_mutex_map[stripe_id] = std::make_unique<std::mutex>();
                }
            }
            stripe_lk = std::unique_lock<std::mutex>(*stripe_mutex_map[stripe_id]);

            // 等待全局并发度许可
            {
                std::unique_lock<std::mutex> lk(concurrency_mu);
                concurrency_cv.wait(lk, [&] { return active_stripes < MAX_CONCURRENT_STRIPES; });
                ++active_stripes;
            }

            std::cout << "[CoRD batch] line " << lno << " stripe_id=" << stripe_id
                      << " ranges=" << range_cnt << " ... (active=" << active_stripes << ")" << std::endl;

            const auto req_t0 = std::chrono::steady_clock::now();
            const bool ok = client.cord_update(stripe_id, logical_ranges, nullptr, 0);
            const auto req_t1 = std::chrono::steady_clock::now();
            const double req_wall_sec = std::chrono::duration<double>(req_t1 - req_t0).count();
            res.wall_sec = req_wall_sec;

            // 释放并发度许可
            {
                std::lock_guard<std::mutex> lk(concurrency_mu);
                --active_stripes;
                concurrency_cv.notify_all();
            }

            if (!ok) {
                std::cout << "[CoRD batch] line " << lno << " FAILED wall_sec=" << std::fixed
                          << std::setprecision(6) << req_wall_sec << std::endl;
                res.success = false;
                return res;
            }

            res.success = true;
            std::cout << "[CoRD batch] line " << lno << " OK wall_sec=" << std::fixed
                      << std::setprecision(6) << req_wall_sec << std::endl;
            return res;
        };

        // 先顺序读取所有行，启动并行任务
        std::vector<std::pair<std::string, int>> pending_lines;
        while (std::getline(trace_file, line)) {
            ++line_no;
            if (is_blank_or_comment_line(line)) continue;
            pending_lines.emplace_back(line, line_no);
        }

        // 使用 std::async 启动任务
        for (auto& pl : pending_lines) {
            futures.push_back(std::async(std::launch::async, process_one_line, pl.first, pl.second));
        }

        // 收集结果
        for (auto& fut : futures) {
            RequestResult r = fut.get();
            if (!batch_has_any) {
                batch_wall_start = std::chrono::steady_clock::now();
                batch_has_any = true;
            }
            batch_wall_end = std::chrono::steady_clock::now();

            if (!r.success) {
                ++total_failures;
                continue;
            }
            ++success_count;
            success_wall_sum += r.wall_sec;
            per_success_wall_sec.push_back(r.wall_sec);
        }
        // ===== 并行 batch 处理结束 =====
        std::cout << "=== CoRD batch summary ===" << std::endl;
        std::cout << "trace_file=" << trace_path << std::endl;
        for (size_t i = 0; i < per_success_wall_sec.size(); ++i) {
            std::cout << "  success[" << i << "] wall_sec=" << std::fixed << std::setprecision(6)
                      << per_success_wall_sec[i] << std::endl;
        }
        std::cout << "success_count=" << success_count << std::endl;
        std::cout << "total_failures=" << total_failures << std::endl;
        std::cout << "per_request_sum_wall_sec=" << std::fixed << std::setprecision(6) << success_wall_sum << "  // sum of individual request times" << std::endl;
        const double batch_real_wall_sec = batch_has_any ? std::chrono::duration<double>(batch_wall_end - batch_wall_start).count() : 0.0;
        std::cout << "batch_real_wall_sec=" << std::fixed << std::setprecision(6) << batch_real_wall_sec << "  // from first start to last end (wall clock)" << std::endl;
        if (success_count > 0) {
            const double avg_wall_sec_per_request = success_wall_sum / static_cast<double>(success_count);
            std::cout << "avg_wall_sec_per_request=" << std::fixed << std::setprecision(6)
                      << avg_wall_sec_per_request << std::endl;
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
