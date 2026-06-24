#ifndef CONFIG_H
#define CONFIG_H

#include "devcommon.h"
#include <cstdint>

namespace ECProject
{
  const int DATANODE_PORT_SHIFT = 500;
  const int PROXY_PORT_SHIFT = 100;
  /** Parix schedule payload TCP uses a dedicated port (append/set keep PROXY_PORT_SHIFT only). */
  const int PARIX_SCHEDULE_TCP_PORT_OFFSET = 1;

  inline int parix_schedule_tcp_port(int proxy_grpc_port)
  {
    return proxy_grpc_port + PROXY_PORT_SHIFT + PARIX_SCHEDULE_TCP_PORT_OFFSET;
  }

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
    /** main_client / bench: number of stripes to create via Client::set() (see parameterConfiguration.xml). */
    int ClientStripeNum = 100;
    /**
     * Parix placement only (planParixFullStripe master data block in [0,k)).
     * Non-zero: mt19937(seed) — same seed and call order => reproducible master picks across runs.
     * 0: legacy rand_num() per call (non-deterministic). See parameterConfiguration.xml.
     */
    std::uint32_t ParixPlacementSeed = 12345;
  };
}

#endif // CONFIG_H