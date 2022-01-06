#include "Serializer.hpp"

#include <endian.h>

#include "BitCast.hpp"
#include "TransactionTypes.hpp"

namespace TPCC
{
static constexpr bool hostArchLE = true;

void push32s(std::vector<uint8_t>& buf, int32_t data)
{
   uint32_t d;
   if constexpr (hostArchLE) {
      d = htole32(data);
   } else {
      d = htobe32(data);
   }
   // NOTE: technically UB
   buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 4);
}

void push64(std::vector<uint8_t>& buf, uint64_t data)
{
   uint32_t d;
   if constexpr (hostArchLE) {
      d = htole64(data);
   } else {
      d = htobe64(data);
   }
   // NOTE: technically UB
   buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 8);
}

void serializeNewOrder(std::vector<uint8_t>& buf,
                       Integer w_id,
                       Integer d_id,
                       Integer c_id,
                       const std::vector<Integer>& lineNumbers,
                       const std::vector<Integer>& supwares,
                       const std::vector<Integer>& itemids,
                       const std::vector<Integer>& qtys,
                       Timestamp timestamp)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push64(buf, timestamp);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_id);
   for (auto val : lineNumbers)
      push32s(buf, val);
   for (auto val : supwares)
      push32s(buf, val);
   for (auto val : itemids)
      push32s(buf, val);
   for (auto val : qtys)
      push32s(buf, val);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::newOrder);
   buf[3] = lineNumbers.size();
}

void serializeDelivery(std::vector<uint8_t>& buf, Integer w_id, Integer carrier_id, Timestamp datetime)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push64(buf, datetime);
   push32s(buf, w_id);
   push32s(buf, carrier_id);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::delivery);
}

void serializeStockLevel(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer threshold)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, threshold);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::stockLevel);
}

void serializeOrderStatusId(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer c_id)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_id);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::orderStatusId);
}
void serializeOrderStatusName(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Varchar<16> c_last)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push32s(buf, w_id);
   push32s(buf, d_id);
   buf.insert(buf.end(), c_last.data, c_last.data + c_last.length);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::orderStatusName);
   buf[3] = c_last.length;
}

void serializePaymentById(std::vector<uint8_t>& buf,
                          Integer w_id,
                          Integer d_id,
                          Integer c_w_id,
                          Integer c_d_id,
                          Integer c_id,
                          Timestamp h_date,
                          Numeric h_amount,
                          Timestamp datetime)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push64(buf, h_date);
   push64(buf, bit_cast<uint64_t>(h_amount));
   push64(buf, datetime);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_w_id);
   push32s(buf, c_d_id);
   push32s(buf, c_id);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::paymentById);
}

void serializePaymentByName(std::vector<uint8_t>& buf,
                            Integer w_id,
                            Integer d_id,
                            Integer c_w_id,
                            Integer c_d_id,
                            Varchar<16> c_last,
                            Timestamp h_date,
                            Numeric h_amount,
                            Timestamp datetime)
{
   buf.assign(8, 0);  // 8 byte placeholder and padding
   push64(buf, h_date);
   push64(buf, bit_cast<uint64_t>(h_amount));
   push64(buf, datetime);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_w_id);
   push32s(buf, c_d_id);
   buf.insert(buf.end(), c_last.data, c_last.data + c_last.length);
   buf[0] = buf.size() >> 8;
   buf[1] = buf.size();
   buf[2] = static_cast<uint8_t>(TransactionType::paymentByName);
   buf[3] = c_last.length;
}
}  // namespace TPCC
