#include "config.h"
#include "tinyxml2.h"
#include <algorithm>
#include <cassert>

namespace ECProject
{
  Config *Config::instance = nullptr;

  Config::Config(const std::string &configPath)
  {
    loadConfig(configPath);
    printConfigs();
    validateConfig();
  }

  void Config::validateConfig() const
  {
    assert(BlockSize % UnitSize == 0 && "Error: BlockSize must be divisible by UnitSize");
    assert((AppendMode == "REP_MODE" || AppendMode == "UNILRC_MODE" || AppendMode == "CACHED_MODE") && "Error: AppendMode must be REP_MODE, UNILRC_MODE, or CACHED_MODE");
    assert((CodeType == "UniLRC" || CodeType == "AzureLRC" || CodeType == "RandomLRC" || CodeType == "SplitParityLRC" || CodeType == "CordXueLRC" || CodeType == "OptimalLRC" || CodeType == "UniformLRC" || CodeType == "XueLRC") && "Error: CodeType must be UniLRC, AzureLRC, RandomLRC, SplitParityLRC, CordXueLRC, OptimalLRC, UniformLRC, or XueLRC");
    assert(DatanodeNumPerCluster > 0 && "Error: DatanodeNumPerCluster must be greater than 0");
    assert(ClusterNum > 0 && "Error: ClusterNum must be greater than 0");
    assert(ClientStripeNum > 0 && "Error: ClientStripeNum must be greater than 0");
    assert(CordRequestTimeoutSec > 0 && "Error: CordRequestTimeoutSec must be greater than 0");
    if (CodeType == "UniLRC")
    {
      assert(DatanodeNumPerCluster > n / z && "Error: DatanodeNumPerCluster must be greater than n / z");
      assert(ClusterNum > z && "Error: ClusterNum must be greater than z");
    }
    if (CodeType == "AzureLRC" || CodeType == "RandomLRC")
    {
      assert(DatanodeNumPerCluster > k / z + 1 && "Error: DatanodeNumPerCluster must be greater than k / z + 1");
      assert(ClusterNum > z + 1 && "Error: ClusterNum must be greater than z + 1");
    }
    if (CodeType == "SplitParityLRC")
    {
      assert(DatanodeNumPerCluster > k / z + 1 && "Error: DatanodeNumPerCluster must be greater than k / z + 1");
      assert(ClusterNum >= 6 && "Error: SplitParityLRC requires ClusterNum >= 6");
      assert(k <= 4 * (r + 1) && "Error: SplitParityLRC requires k <= 4*(r+1)");
    }
    if (CodeType == "CordXueLRC")
    {
      assert(k > 0 && r > 0 && z > 0 && "Error: CordXueLRC requires k, r, z > 0");
      // 允许 (k+r)%z != 0：本地组槽位按 floor/ceil 分配（较大组在末尾），全局块仍全部放入最后一组。
      // 因此要求最后一组槽位数足以容纳 r 个全局块。
      const int last_group_slots = (k + r) / z + (((k + r) % z) > 0 ? 1 : 0);
      assert(last_group_slots >= r &&
             "Error: CordXueLRC requires last local group slots >= r");
      // Cord 放置会把本地组拆到多个 cluster，单 cluster 最密约：
      // global: r+z；primary/batch: r+1；remainder: 最多 z*r。
      // 同 stripe 同 cluster 要求不同 datanode，故节点数需大于该上界。
      const int max_blocks_per_cluster = std::max(r + z, std::max(r + 1, z * r));
      assert(DatanodeNumPerCluster > max_blocks_per_cluster &&
             "Error: DatanodeNumPerCluster must be greater than max CordXueLRC blocks placed in any cluster");
      assert(ClusterNum > z + 1 && "Error: CordXueLRC requires ClusterNum > z + 1");
    }
    if (CodeType == "OptimalLRC")
    {
      assert(DatanodeNumPerCluster > r + 1 && "Error: DatanodeNumPerCluster must be greater than r + 1");
      assert(ClusterNum > std::ceil(1.0 * k / z / (r + 1)) * z + 1 && "Error: ClusterNum must be greater than std::ceil(1.0 * k / z / (r + 1)) * z + 1");
    }
    if (CodeType == "UniformLRC")
    {
      assert(DatanodeNumPerCluster > r && "Error: DatanodeNumPerCluster must be greater than r");
      assert(ClusterNum > ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z)) && "Error: ClusterNum must be greater than ((((k + r) / z + 1) / (r + 1) + (bool)(((k + r) / z + 1) % (r + 1))) * ((k + r) % z)) + (((k + r) / z) / (r + 1) + (bool)(((k + r) / z) % (r + 1))) * (z - ((k + r) % z))");
    }
    if (CodeType == "XueLRC")
    {
      assert(r > 0 && z > 0 && "Error: XueLRC requires r > 0 and z > 0");
      assert(k % r == 0 && "Error: XueLRC requires k % r == 0");
      assert(DatanodeNumPerCluster > z && "Error: DatanodeNumPerCluster should be greater than z");
      assert(ClusterNum > 1 && "Error: XueLRC requires at least 2 clusters");
    }
  }

  Config *Config::getInstance(const std::string &configPath)
  {
    if (instance == nullptr)
    {
      instance = new Config(configPath);
    }
    return instance;
  }

  void Config::loadConfig(const std::string &configPath)
  {
    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(configPath.c_str()) != tinyxml2::XML_SUCCESS)
    {
      std::cerr << "Failed to load config file: " << configPath << std::endl;
      return;
    }

    tinyxml2::XMLElement *root = doc.RootElement();
    if (root == nullptr)
    {
      std::cerr << "Invalid config file format" << std::endl;
      return;
    }

    if (auto elem = root->FirstChildElement("AlignedSize"))
      AlignedSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("UnitSize"))
      UnitSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("BlockSize"))
      BlockSize = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("z"))
      z = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CodeType"))
      CodeType = std::string(elem->GetText());
    if (CodeType == "UniLRC")
    {
      if (auto elem = root->FirstChildElement("alpha"))
        alpha = std::stoi(elem->GetText());
      k = alpha * z * z - alpha * z;
      r = alpha * z;
    }
    else
    {
      if (auto elem = root->FirstChildElement("k"))
        k = std::stoi(elem->GetText());
      if (auto elem = root->FirstChildElement("r"))
        r = std::stoi(elem->GetText());
    }
    n = k + r + z;

    if (auto elem = root->FirstChildElement("DatanodeNumPerCluster"))
      DatanodeNumPerCluster = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("ClusterNum"))
      ClusterNum = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CoordinatorIP"))
      CoordinatorIP = std::string(elem->GetText());
    if (auto elem = root->FirstChildElement("CoordinatorPort"))
      CoordinatorPort = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("AppendMode"))
      AppendMode = std::string(elem->GetText());
    if (auto elem = root->FirstChildElement("PlacementRandomSeed"))
      PlacementRandomSeed = std::stoull(elem->GetText());
    if (auto elem = root->FirstChildElement("ClientStripeNum"))
      ClientStripeNum = std::stoi(elem->GetText());
    if (auto elem = root->FirstChildElement("CordRequestTimeoutSec"))
      CordRequestTimeoutSec = std::stoi(elem->GetText());
  }

  void Config::printConfigs() const
  {
    std::cout << "Configuration Parameters:" << std::endl;
    std::cout << "  AlignedSize: " << AlignedSize << " bytes" << std::endl;
    std::cout << "  UnitSize: " << UnitSize << " bytes" << std::endl;
    std::cout << "  BlockSize: " << BlockSize << " bytes" << std::endl;
    std::cout << "  alpha: " << (int)alpha << std::endl;
    std::cout << "  z: " << (int)z << std::endl;
    std::cout << "  n: " << n << std::endl;
    std::cout << "  k: " << k << std::endl;
    std::cout << "  r: " << r << std::endl;
    std::cout << "  (n, k, r, z): (" << n << ", " << k << ", " << r << ", " << (int)z << ")" << std::endl;
    std::cout << "  DatanodeNumPerCluster: " << DatanodeNumPerCluster << " nodes/cluster" << std::endl;
    std::cout << "  ClusterNum: " << (int)ClusterNum << " clusters" << std::endl;
    std::cout << "  CoordinatorIP: " << CoordinatorIP << std::endl;
    std::cout << "  CoordinatorPort: " << CoordinatorPort << std::endl;
    std::cout << "  AppendMode: " << AppendMode << std::endl;
    std::cout << "  CodeType: " << CodeType << std::endl;
    std::cout << "  PlacementRandomSeed: " << PlacementRandomSeed << std::endl;
    std::cout << "  ClientStripeNum: " << ClientStripeNum << std::endl;
    std::cout << "  CordRequestTimeoutSec: " << CordRequestTimeoutSec << " s" << std::endl;
  }
}
