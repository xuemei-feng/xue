#include "toolbox.h"
#include <sstream>
#include <ctime>
#include <cstring>
#include <random>
#include <unordered_set>
#include <cassert>

namespace ECProject
{

  ToolBox *ToolBox::instance = nullptr;

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const size_t str_size, const std::vector<size_t> &sizes)
  {
    std::vector<char *> result;
    size_t currentOffset = 0;

    for (size_t i = 0; i < sizes.size(); ++i)
    {
      // slice size is valid
      // std::cout << "[ToolBox::splitCharPointer] currentOffset: " << currentOffset << " i: " << i << " sizes[i]: " << sizes[i] << " str_size: " << str_size << std::endl;
      assert(currentOffset + sizes[i] <= str_size && "splitCharPointer: Invalid offset provided.");
      result.push_back(const_cast<char *>(str + currentOffset));
      currentOffset += sizes[i];
    }
    assert(currentOffset == str_size && "The buf is not fully devided!");

    return result;
  }

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const std::shared_ptr<proxy_proto::AppendStripeDataPlacement> append_stripe_data_placement)
  {
    std::vector<size_t> sizes;
    for (int i = 0; i < append_stripe_data_placement->sizes_size(); i++)
    {
      sizes.push_back(append_stripe_data_placement->sizes(i));
    }
    return splitCharPointer(str, append_stripe_data_placement->append_size(), sizes);
  }

  std::vector<char *> ToolBox::splitCharPointer(const char *str, const coordinator_proto::ReplyProxyIPsPorts *reply_proxy_ips_ports)
  {
    std::vector<size_t> sizes;
    for (int i = 0; i < reply_proxy_ips_ports->cluster_slice_sizes_size(); i++)
    {
      sizes.push_back(reply_proxy_ips_ports->cluster_slice_sizes(i));
    }
    return splitCharPointer(str, reply_proxy_ips_ports->sum_append_size(), sizes);
  }

  std::string ToolBox::gen_append_key(int stripe_id, int group_id)
  {
    return std::to_string(stripe_id) + "_" + std::to_string(group_id);
  }

  namespace
  {
    void format_block_id_list(std::ostringstream &oss, const std::vector<int> &block_ids)
    {
      for (size_t i = 0; i < block_ids.size(); ++i)
      {
        if (i > 0)
        {
          oss << ",";
        }
        oss << block_ids[i];
      }
    }
  }

  std::string ToolBox::gen_append_key_cluster_plan(int stripe_id, int group_id, int cluster_id,
                                                   const std::vector<int> &tcp_block_ids,
                                                   const std::vector<int> &meta_block_ids)
  {
    std::ostringstream oss;
    oss << stripe_id << "_" << group_id << "c" << cluster_id << "#";
    format_block_id_list(oss, tcp_block_ids);
    if (!meta_block_ids.empty())
    {
      oss << "@";
      format_block_id_list(oss, meta_block_ids);
    }
    return oss.str();
  }

  std::string ToolBox::gen_append_key_cluster_blocks(int stripe_id, int group_id, int cluster_id,
                                                     const std::vector<int> &block_ids)
  {
    return gen_append_key_cluster_plan(stripe_id, group_id, cluster_id, block_ids, {});
  }

  bool ToolBox::parse_append_key_tcp_block_ids(const std::string &key, std::vector<int> *block_ids)
  {
    if (block_ids == nullptr)
    {
      return false;
    }
    block_ids->clear();
    const size_t hash_pos = key.find('#');
    if (hash_pos == std::string::npos)
    {
      return false;
    }
    const size_t at_pos = key.find('@', hash_pos + 1);
    const size_t end = (at_pos == std::string::npos) ? key.size() : at_pos;
    if (hash_pos + 1 >= end)
    {
      return false;
    }
    std::stringstream ss(key.substr(hash_pos + 1, end - hash_pos - 1));
    std::string token;
    while (std::getline(ss, token, ','))
    {
      if (!token.empty())
      {
        block_ids->push_back(std::stoi(token));
      }
    }
    return !block_ids->empty();
  }

  bool ToolBox::parse_append_key_meta_block_ids(const std::string &key, std::vector<int> *block_ids)
  {
    if (block_ids == nullptr)
    {
      return false;
    }
    block_ids->clear();
    const size_t at_pos = key.find('@');
    if (at_pos == std::string::npos || at_pos + 1 >= key.size())
    {
      return false;
    }
    std::stringstream ss(key.substr(at_pos + 1));
    std::string token;
    while (std::getline(ss, token, ','))
    {
      if (!token.empty())
      {
        block_ids->push_back(std::stoi(token));
      }
    }
    return !block_ids->empty();
  }

  bool ToolBox::random_generate_kv(std::string &key, std::string &value,
                                   int key_length, int value_length)
  {
    /*如果长度为0，则随机生成长度,key的长度不大于MAX_KEY_LENGTH，value的长度不大于MAX_VALUE_LENGTH*/
    /*现在生成的key,value内容是固定的，可以改成随机的(增加参数)*/
    /*如果需要生成的key太多，避免重复生成，可以改成写文件保存下来keyvalue，下次直接读文件的形式，
    但这个需要修改函数参数或者修改run_client的内容了*/

    struct timespec tp;

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tp);
    srand(tp.tv_nsec);
    if (key_length == 0)
    {
    }
    else
    {
      for (int i = 0; i < key_length; i++)
      {
        key = key + char('a' + rand() % 26);
      }
    }
    if (value_length == 0)
    {
    }
    else
    {
      for (int i = 0; i < value_length / 26; i++)
      {
        for (int j = 65; j <= 90; j++)
        {
          value = value + char(j);
        }
      }
      for (int i = 0; i < value_length - int(value.size()); i++)
      {
        value = value + char('A' + i);
      }
    }
    return true;
  }

  std::vector<unsigned char> ToolBox::int_to_bytes(int integer)
  {
    std::vector<unsigned char> bytes(sizeof(int));
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++)
    {
      memcpy(&bytes[i], p + i, 1);
    }
    return bytes;
  }

  int ToolBox::bytes_to_int(std::vector<unsigned char> &bytes)
  {
    int integer;
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++)
    {
      memcpy(p + i, &bytes[i], 1);
    }
    return integer;
  }

  bool ToolBox::random_generate_value(std::string &value, int value_length)
  {
    /*生成一个固定大小的随机value*/
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, 25);
    for (int i = 0; i < value_length; i++)
    {
      value = value + (dis(gen) % 2 ? char('a' + dis(gen) % 26) : char('A' + dis(gen) % 26));
    }
    return true;
  }
  std::string ToolBox::gen_key(int key_len, std::unordered_set<std::string> keys)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> dis(0, 25);
    std::string key;
    do
    {
      key.clear();
      for (int i = 0; i < key_len; i++)
      {
        key = key + (dis(gen) % 2 ? char('a' + dis(gen) % 26) : char('A' + dis(gen) % 26));
      }
    } while (keys.count(key) > 0);
    return key;
  }

  void ToolBox::remove_common_zeros(std::vector<int>& vec1, std::vector<int>& vec2, std::vector<int>& vec3) {
    // 检查三个vector大小是否相同
    if (vec1.size() != vec2.size() || vec2.size() != vec3.size()) {
        std::cerr << "错误：三个vector大小不同！" << std::endl;
        return;
    }
    
    // 存储需要删除的索引
    std::vector<size_t> indices_to_remove;
    
    // 遍历所有索引，找出所有三个元素都为0的位置
    for (size_t i = 0; i < vec1.size(); ++i) {
        if (vec1[i] == 0 && vec2[i] == 0 && vec3[i] == 0) {
            indices_to_remove.push_back(i);
        }
    }
    
    // 从后往前删除元素（避免索引变化问题）
    std::sort(indices_to_remove.rbegin(), indices_to_remove.rend());
    for (size_t index : indices_to_remove) {
        vec1.erase(vec1.begin() + index);
        vec2.erase(vec2.begin() + index);
        vec3.erase(vec3.begin() + index);
    }
}
} // namespace ECProject