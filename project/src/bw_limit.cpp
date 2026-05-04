#include "bw_limit.h"
#include <fstream>
#include <sstream>
#include <algorithm>

namespace ECProject
{
  std::string BWLimit::makeKey(int a, int b)
  {
    if (a > b)
    {
      std::swap(a, b);
    }
    return std::to_string(a) + "," + std::to_string(b);
  }

  std::string BWLimit::MakeOrderedPairKey(int a, int b)
  {
    return makeKey(a, b);
  }

  bool BWLimit::loadFromFile(const std::string &file_path)
  {
    m_bw.clear();
    std::ifstream in(file_path);
    if (!in.is_open())
    {
      return false;
    }

    // Simple format:
    // i j bwMBps
    // Example: 0 1 4.21
    std::string line;
    while (std::getline(in, line))
    {
      if (line.empty())
      {
        continue;
      }
      if (line[0] == '#')
      {
        continue;
      }
      std::istringstream iss(line);
      int a = -1, b = -1;
      double bw = 0.0;
      if (!(iss >> a >> b >> bw))
      {
        continue;
      }
      if (a < 0 || b < 0 || bw <= 0.0)
      {
        continue;
      }
      m_bw[makeKey(a, b)] = bw;
    }
    return true;
  }

  double BWLimit::getBandwidthMBps(int cluster_a, int cluster_b, double fallback_bw) const
  {
    const auto it = m_bw.find(makeKey(cluster_a, cluster_b));
    if (it == m_bw.end())
    {
      return fallback_bw;
    }
    return it->second;
  }
}

