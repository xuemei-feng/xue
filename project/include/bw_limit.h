#ifndef BW_LIMIT_H
#define BW_LIMIT_H

#include <string>
#include <unordered_map>

namespace ECProject
{
  class BWLimit
  {
  public:
    BWLimit() = default;
    explicit BWLimit(const std::string &file_path)
    {
      loadFromFile(file_path);
    }

    bool loadFromFile(const std::string &file_path);
    // Return MB/s. If not found, return fallback_bw.
    double getBandwidthMBps(int cluster_a, int cluster_b, double fallback_bw = 0.0) const;

  private:
    std::unordered_map<std::string, double> m_bw;
    static std::string makeKey(int a, int b);
  };
}

#endif

