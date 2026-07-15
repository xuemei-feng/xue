#ifndef CONFIG_H
#define CONFIG_H

#include <cstdint>
#include "devcommon.h"

namespace ECProject
{
  const int DATANODE_PORT_SHIFT = 500;
  const int PROXY_PORT_SHIFT = 100;
  /** CoRD proxy↔proxy delta 直连 TCP：grpc_port + PROXY_PORT_SHIFT + 此偏移（与 client 数据口 +100 错开） */
  const int PROXY_XFER_PORT_SUB_OFFSET = 1;
  /** SET/Append 数据直连 TCP 端口偏移：grpc_port + 此偏移（独立于 CoRD 的 shared acceptor 端口） */
  const int SET_XFER_PORT_OFFSET = 150;
  /** Recovery 跨 rack 数据直连 TCP 端口偏移：grpc_port + 此偏移（独立于 SET/CoRD 端口） */
  const int RECOVERY_XFER_PORT_OFFSET = 200;

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
    /** 0: placement uses random_device where applicable. Non-zero: deterministic placement for same seed, stripe_id, topology (RandomLRC + encode Ran). */
    std::uint64_t PlacementRandomSeed = 0;
    /** SET 阶段放置的条带数；有效 stripe_id 为 0 .. ClientStripeNum-1 */
    int ClientStripeNum = 100;
    /** CoRD 单次 update 总超时（秒），含 plan、upload、跨 cluster 传输等待 */
    int CordRequestTimeoutSec = 2;
  };
}

#endif // CONFIG_H
