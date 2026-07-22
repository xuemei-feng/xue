#ifndef ECPROJECT_CORD_CLASS_ALGORITHM_H
#define ECPROJECT_CORD_CLASS_ALGORITHM_H

#include "cord_algorithm2.h"
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ECProject
{
  struct Stripe;
}

namespace ECProject
{
  namespace cord_class
  {
    enum class CordDataBlockClass
    {
      CLASS1 = 1,
      CLASS2 = 2,
      CLASS3 = 3,
    };

    struct CordIngressParityWriteRec
    {
      int block_id = -1;
      std::string block_key;
      std::string datanode_ip;
      int datanode_port = 0;
      int stripe_group = -1;
    };

    struct CordIngressClusterHints
    {
      std::vector<CordIngressParityWriteRec> local_parity_writes;
      std::vector<CordIngressParityWriteRec> global_parity_writes;
      std::vector<int32_t> cache_lp_stripe_groups;
    };

    /** CordXueLRC：按 class1/2/3 生成 train_route 并调度 timeslot */
    cord_alg2::Algorithm2Result build_class_update_plan(
        const Stripe &stripe,
        const std::map<int, std::vector<std::pair<int, int>>> &block_intervals,
        int cluster_num,
        const cord_alg2::TransferParams &tp,
        std::map<int, CordIngressClusterHints> *ingress_hints_by_cluster);

    CordDataBlockClass classify_data_block(const Stripe &stripe, int data_block_id);
  } // namespace cord_class
} // namespace ECProject

#endif
