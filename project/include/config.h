#ifndef CONFIG_H
#define CONFIG_H

#include <cstdint>
#include "devcommon.h"

namespace ECProject
{
  const int DATANODE_PORT_SHIFT = 500;
  const int PROXY_PORT_SHIFT = 100; 

  class Config
  {
  private:
    static Config *instance;
    Config(const std::string &configPath);

  public:
    static Config *getInstance(const std::string &configPath);
    void loadConfig(const std::string &configPath);
    void printConfigs() const;
    void validateConfig() const;

    int AlignedSize = 4096;
    int UnitSize = 8 * 1024;
    unsigned int BlockSize = 64 * 1024;
    int alpha = 2;
    int z = 2;
    // TODO: need to modify configs to support directly setting k,r,z
    int n = alpha * z * z + z;
    int k = alpha * z * z - alpha * z;
    int r = alpha * z;
    int DatanodeNumPerCluster = 0;
    int ClusterNum = 0;
    std::string CoordinatorIP = "0.0.0.0";
    int CoordinatorPort = 55555;
    std::string AppendMode = "UNILRC_MODE";
    std::string CodeType = "UniLRC";
    /** 0: RandomLRC uses random_device (non-reproducible). Non-zero: deterministic placement for the same seed, stripe_id, and cluster layout. */
    std::uint64_t PlacementRandomSeed = 0;
  };
}

#endif // CONFIG_H
