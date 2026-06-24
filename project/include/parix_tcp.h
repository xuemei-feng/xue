#ifndef ECPROJECT_PARIX_TCP_H
#define ECPROJECT_PARIX_TCP_H

#include <cstdint>

namespace ECProject
{

  /** Magic for parixScheduleDataUpdate side-channel TCP (multi-client safe framing). */
  inline constexpr uint32_t kParixScheduleTcpMagic = 0x50525831u; // "PRX1"

#pragma pack(push, 1)
  struct ParixScheduleTcpHeader
  {
    uint32_t magic = kParixScheduleTcpMagic;
    uint32_t data_block_id = 0;
    uint64_t batch_id = 0;
    uint64_t payload_len = 0;
  };
#pragma pack(pop)

  static_assert(sizeof(ParixScheduleTcpHeader) == 24, "ParixScheduleTcpHeader must be 24 bytes");

  inline ParixScheduleTcpHeader make_parix_schedule_tcp_header(uint64_t batch_id, uint32_t data_block_id, uint64_t payload_len)
  {
    ParixScheduleTcpHeader h;
    h.magic = kParixScheduleTcpMagic;
    h.data_block_id = data_block_id;
    h.batch_id = batch_id;
    h.payload_len = payload_len;
    return h;
  }

} // namespace ECProject

#endif
