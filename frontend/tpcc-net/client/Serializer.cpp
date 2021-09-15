#include "Serializer.hpp"

#include <endian.h>

#include "BitCast.hpp"

namespace TPCC
{
void push32s(std::vector<uint8_t>& buf, int32_t data)
{
   auto d = htobe32(data);
   // NOTE: technically UB
   buf.insert(buf.end(), reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 4);
}

void push64(std::vector<uint8_t>& buf, uint64_t data)
{
   auto d = htobe64(data);
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
   buf.push_back(1);
   buf.push_back(lineNumbers.size());
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
   push64(buf, timestamp);
}

void serializeDelivery(std::vector<uint8_t>& buf, Integer w_id, Integer carrier_id, Timestamp datetime)
{
   buf.push_back(2);
   push32s(buf, w_id);
   push32s(buf, carrier_id);
   push64(buf, datetime);
}

void serializeStockLevel(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer threshold)
{
   buf.push_back(3);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, threshold);
}

void serializeOrderStatusId(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer c_id)
{
   buf.push_back(4);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_id);
}
void serializeOrderStatusName(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Varchar<16> c_last)
{
   buf.push_back(5);
   buf.push_back(c_last.length);
   push32s(buf, w_id);
   push32s(buf, d_id);
   buf.insert(buf.end(), c_last.data, c_last.data + c_last.length);
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
   buf.push_back(6);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_w_id);
   push32s(buf, c_d_id);
   push32s(buf, c_id);
   push64(buf, h_date);
   push64(buf, bit_cast<uint64_t>(h_amount));
   push64(buf, datetime);
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
   buf.push_back(7);
   buf.push_back(c_last.length);
   push32s(buf, w_id);
   push32s(buf, d_id);
   push32s(buf, c_w_id);
   push32s(buf, c_d_id);
   buf.insert(buf.end(), c_last.data, c_last.data + c_last.length);
   push64(buf, h_date);
   push64(buf, bit_cast<uint64_t>(h_amount));
   push64(buf, datetime);
}
}  // namespace TPCC
