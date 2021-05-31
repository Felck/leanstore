#include <vector>

#include "types.hpp"

namespace TPCC
{
void serializeNewOrder(std::vector<uint8_t>& buf,
                       Integer w_id,
                       Integer d_id,
                       Integer c_id,
                       const std::vector<Integer>& lineNumbers,
                       const std::vector<Integer>& supwares,
                       const std::vector<Integer>& itemids,
                       const std::vector<Integer>& qtys,
                       Timestamp timestamp);
void serializeDelivery(std::vector<uint8_t>& buf, Integer w_id, Integer carrier_id, Timestamp datetime);
void serializeStockLevel(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer threshold);
void serializeOrderStatusId(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Integer c_id);
void serializeOrderStatusName(std::vector<uint8_t>& buf, Integer w_id, Integer d_id, Varchar<16> c_last);
void serializePaymentById(std::vector<uint8_t>& buf,
                          Integer w_id,
                          Integer d_id,
                          Integer c_w_id,
                          Integer c_d_id,
                          Integer c_id,
                          Timestamp h_date,
                          Numeric h_amount,
                          Timestamp datetime);
void serializePaymentByName(std::vector<uint8_t>& buf,
                            Integer w_id,
                            Integer d_id,
                            Integer c_w_id,
                            Integer c_d_id,
                            Varchar<16> c_last,
                            Timestamp h_date,
                            Numeric h_amount,
                            Timestamp datetime);
}  // namespace TPCC
