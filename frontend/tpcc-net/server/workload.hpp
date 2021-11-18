#include <gflags/gflags.h>

#include "OBuffer.hpp"
#include "adapter.hpp"
#include "schema.hpp"

DECLARE_bool(order_wdc_index);
DECLARE_int32(tpcc_abort_pct);
DECLARE_bool(tpcc_remove);
DECLARE_uint32(tpcc_warehouse_count);

namespace TPCC
{
extern LeanStoreAdapter<warehouse_t> warehouse;
extern LeanStoreAdapter<district_t> district;
extern LeanStoreAdapter<customer_t> customer;
extern LeanStoreAdapter<customer_wdl_t> customerwdl;
extern LeanStoreAdapter<history_t> history;
extern LeanStoreAdapter<neworder_t> neworder;
extern LeanStoreAdapter<order_t> order;
extern LeanStoreAdapter<order_wdc_t> order_wdc;
extern LeanStoreAdapter<orderline_t> orderline;
extern LeanStoreAdapter<item_t> item;
extern LeanStoreAdapter<stock_t> stock;

Integer urand(Integer low, Integer high);

void loadItem();
void loadWarehouse();
void loadStock(Integer w_id);
void loadDistrinct(Integer w_id);
void loadCustomer(Integer w_id, Integer d_id);
void loadOrders(Integer w_id, Integer d_id);

void newOrder(Integer w_id,
              Integer d_id,
              Integer c_id,
              const vector<Integer>& lineNumbers,
              const vector<Integer>& supwares,
              const vector<Integer>& itemids,
              const vector<Integer>& qtys,
              Timestamp timestamp,
              Net::OBuffer<uint8_t>& oBuffer);
void delivery(Integer w_id, Integer carrier_id, Timestamp datetime);
void stockLevel(Integer w_id, Integer d_id, Integer threshold, Net::OBuffer<uint8_t>& oBuffer);
void orderStatusId(Integer w_id, Integer d_id, Integer c_id, Net::OBuffer<uint8_t>& oBuffer);
void orderStatusName(Integer w_id, Integer d_id, Varchar<16> c_last, Net::OBuffer<uint8_t>& oBuffer, size_t resetIdx);
void paymentById(Integer w_id,
                 Integer d_id,
                 Integer c_w_id,
                 Integer c_d_id,
                 Integer c_id,
                 Timestamp h_date,
                 Numeric h_amount,
                 Timestamp datetime,
                 Net::OBuffer<uint8_t>& oBuffer);
void paymentByName(Integer w_id,
                   Integer d_id,
                   Integer c_w_id,
                   Integer c_d_id,
                   Varchar<16> c_last,
                   Timestamp h_date,
                   Numeric h_amount,
                   Timestamp datetime,
                   Net::OBuffer<uint8_t>& oBuffer,
                   size_t resetIdx);
}  // namespace TPCC
