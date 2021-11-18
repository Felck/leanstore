#include "workload.hpp"

#include "TransactionTypes.hpp"

DEFINE_bool(order_wdc_index, true, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_bool(tpcc_remove, true, "");
DEFINE_uint32(tpcc_warehouse_count, 1, "");

constexpr INTEGER OL_I_ID_C = 7911;  // in range [0, 8191]
constexpr INTEGER C_ID_C = 259;      // in range [0, 1023]
// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
// be within [65, 119]
constexpr INTEGER C_LAST_LOAD_C = 157;  // in range [0, 255]
constexpr INTEGER C_LAST_RUN_C = 223;   // in range [0, 255]

constexpr INTEGER ITEMS_NO = 100000;  // 100K

namespace TPCC
{
LeanStoreAdapter<warehouse_t> warehouse;
LeanStoreAdapter<district_t> district;
LeanStoreAdapter<customer_t> customer;
LeanStoreAdapter<customer_wdl_t> customerwdl;
LeanStoreAdapter<history_t> history;
LeanStoreAdapter<neworder_t> neworder;
LeanStoreAdapter<order_t> order;
LeanStoreAdapter<order_wdc_t> order_wdc;
LeanStoreAdapter<orderline_t> orderline;
LeanStoreAdapter<item_t> item;
LeanStoreAdapter<stock_t> stock;

// [0, n)
Integer rnd(Integer n)
{
   return leanstore::utils::RandomGenerator::getRand(0, n);
}

// [fromId, toId]
Integer randomId(Integer fromId, Integer toId)
{
   return leanstore::utils::RandomGenerator::getRand(fromId, toId + 1);
}

// [low, high]
Integer urand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

Integer urandexcept(Integer low, Integer high, Integer v)
{
   if (high <= low)
      return low;
   Integer r = rnd(high - low) + low;
   if (r >= v)
      return r + 1;
   else
      return r;
}

template <int maxLength>
Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= maxLength);
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<maxLength> result;
   for (Integer index = 0; index < len; index++) {
      Integer i = rnd(62);
      if (i < 10)
         result.append(48 + i);
      else if (i < 36)
         result.append(64 - 10 + i);
      else
         result.append(96 - 36 + i);
   }
   return result;
}

Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr)
{
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<16> result;
   for (Integer i = 0; i < len; i++)
      result.append(48 + rnd(10));
   return result;
}

Varchar<16> namePart(Integer id)
{
   assert(id < 10);
   Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   return data[id];
}

Varchar<16> genName(Integer id)
{
   return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10);
}

Numeric randomNumeric(Numeric min, Numeric max)
{
   double range = (max - min);
   double div = RAND_MAX / range;
   return min + (leanstore::utils::RandomGenerator::getRandU64() / div);
}

Varchar<9> randomzip()
{
   Integer id = rnd(10000);
   Varchar<9> result;
   result.append(48 + (id / 1000));
   result.append(48 + (id / 100) % 10);
   result.append(48 + (id / 10) % 10);
   result.append(48 + (id % 10));
   return result || Varchar<9>("11111");
}

Integer nurand(Integer a, Integer x, Integer y, Integer C = 42)
{
   // TPC-C random is [a,b] inclusive
   // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
   // return (((rnd(a + 1) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
   return (((urand(0, a) | urand(x, y)) + C) % (y - x + 1)) + x;
   // incorrect: return (((rnd(a) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
}

inline Integer getItemID()
{
   // OL_I_ID_C
   return nurand(8191, 1, ITEMS_NO, OL_I_ID_C);
}
inline Integer getCustomerID()
{
   // C_ID_C
   return nurand(1023, 1, 3000, C_ID_C);
   // return urand(1, 3000);
}
inline Integer getNonUniformRandomLastNameForRun()
{
   // C_LAST_RUN_C
   return nurand(255, 0, 999, C_LAST_RUN_C);
}
inline Integer getNonUniformRandomLastNameForLoad()
{
   // C_LAST_LOAD_C
   return nurand(255, 0, 999, C_LAST_LOAD_C);
}
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
void loadItem()
{
   for (Integer i = 1; i <= ITEMS_NO; i++) {
      Varchar<50> i_data = randomastring<50>(25, 50);
      if (rnd(10) == 0) {
         i_data.length = rnd(i_data.length - 8);
         i_data = i_data || Varchar<10>("ORIGINAL");
      }
      item.insert({i}, {randomId(1, 10000), randomastring<24>(14, 24), randomNumeric(1.00, 100.00), i_data});
   }
}

void loadWarehouse()
{
   for (Integer i = 0; i < static_cast<Integer>(FLAGS_tpcc_warehouse_count); i++) {
      warehouse.insert({i + 1}, {randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                 randomastring<2>(2, 2), randomzip(), randomNumeric(0.1000, 0.2000), 3000000});
   }
}

void loadStock(Integer w_id)
{
   for (Integer i = 0; i < ITEMS_NO; i++) {
      Varchar<50> s_data = randomastring<50>(25, 50);
      if (rnd(10) == 0) {
         s_data.length = rnd(s_data.length - 8);
         s_data = s_data || Varchar<10>("ORIGINAL");
      }
      stock.insert({w_id, i + 1}, {randomNumeric(10, 100), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24),
                                   randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24),
                                   randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), 0, 0, 0, s_data});
   }
}

void loadDistrinct(Integer w_id)
{
   for (Integer i = 1; i < 11; i++) {
      district.insert({w_id, i}, {randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                  randomastring<2>(2, 2), randomzip(), randomNumeric(0.0000, 0.2000), 3000000, 3001});
   }
}

Timestamp currentTimestamp()
{
   return 1;
}

void loadCustomer(Integer w_id, Integer d_id)
{
   Timestamp now = currentTimestamp();
   for (Integer i = 0; i < 3000; i++) {
      Varchar<16> c_last;
      if (i < 1000)
         c_last = genName(i);
      else
         c_last = genName(getNonUniformRandomLastNameForLoad());
      Varchar<16> c_first = randomastring<16>(8, 16);
      Varchar<2> c_credit(rnd(10) ? "GC" : "BC");
      customer.insert({w_id, d_id, i + 1}, {c_first, "OE", c_last, randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                            randomastring<2>(2, 2), randomzip(), randomnstring(16, 16), now, c_credit, 50000.00,
                                            randomNumeric(0.0000, 0.5000), -10.00, 1, 0, 0, randomastring<500>(300, 500)});
      customerwdl.insert({w_id, d_id, c_last, c_first}, {i + 1});
      Integer t_id = (Integer)WorkerCounters::myCounters().t_id;
      Integer h_id = (Integer)WorkerCounters::myCounters().variable_for_workload++;
      history.insert({t_id, h_id}, {i + 1, d_id, w_id, d_id, w_id, now, 10.00, randomastring<24>(12, 24)});
   }
}

void loadOrders(Integer w_id, Integer d_id)
{
   Timestamp now = currentTimestamp();
   vector<Integer> c_ids;
   for (Integer i = 1; i <= 3000; i++)
      c_ids.push_back(i);
   random_shuffle(c_ids.begin(), c_ids.end());
   Integer o_id = 1;
   for (Integer o_c_id : c_ids) {
      Integer o_carrier_id = (o_id < 2101) ? rnd(10) + 1 : 0;
      Numeric o_ol_cnt = rnd(10) + 5;

      order.insert({w_id, d_id, o_id}, {o_c_id, now, o_carrier_id, o_ol_cnt, 1});
      if (FLAGS_order_wdc_index) {
         order_wdc.insert({w_id, d_id, o_c_id, o_id}, {});
      }

      for (Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
         Timestamp ol_delivery_d = 0;
         if (o_id < 2101)
            ol_delivery_d = now;
         Numeric ol_amount = (o_id < 2101) ? 0 : randomNumeric(0.01, 9999.99);
         const Integer ol_i_id = rnd(ITEMS_NO) + 1;
         orderline.insert({w_id, d_id, o_id, ol_number}, {ol_i_id, w_id, ol_delivery_d, 5, ol_amount, randomastring<24>(24, 24)});
      }
      o_id++;
   }

   for (Integer i = 2100; i <= 3000; i++)
      neworder.insert({w_id, d_id, i}, {});
}

// run
void newOrder(Integer w_id,
              Integer d_id,
              Integer c_id,
              const vector<Integer>& lineNumbers,
              const vector<Integer>& supwares,
              const vector<Integer>& itemids,
              const vector<Integer>& qtys,
              Timestamp timestamp,
              Net::OBuffer<uint8_t>& oBuffer)
{
   Numeric c_discount;
   auto rp = oBuffer.setResetPoint();
   customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {
      oBuffer.reset(rp);
      c_discount = rec.c_discount;
      oBuffer.pushVC(rec.c_last);
      oBuffer.pushVC(rec.c_credit);
      oBuffer.pushD(rec.c_discount);
   });
   oBuffer.dropResetPoint(rp);

   Numeric w_tax = warehouse.lookupField({w_id}, &warehouse_t::w_tax);
   Numeric d_tax;
   Integer o_id;

   district.update1(
       {w_id, d_id},
       [&](district_t& rec) {
          d_tax = rec.d_tax;
          o_id = rec.d_next_o_id++;
       },
       WALUpdate1(district_t, d_next_o_id));

   oBuffer.pushD(w_tax);
   oBuffer.pushD(d_tax);
   oBuffer.push32(o_id);
   oBuffer.push64(timestamp);  // o_entry_d

   Numeric all_local = 1;
   for (Integer sw : supwares)
      if (sw != w_id)
         all_local = 0;
   Numeric cnt = lineNumbers.size();
   Integer carrier_id = 0; /*null*/
   order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
   if (FLAGS_order_wdc_index) {
      order_wdc.insert({w_id, d_id, c_id, o_id}, {});
   }
   neworder.insert({w_id, d_id, o_id}, {});

   for (unsigned i = 0; i < lineNumbers.size(); i++) {
      Integer qty = qtys[i];
      stock.update1(
          {supwares[i], itemids[i]},
          [&](stock_t& rec) {
             auto& s_quantity = rec.s_quantity;
             s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
             rec.s_remote_cnt += (supwares[i] != w_id);
             rec.s_order_cnt++;
             rec.s_ytd += qty;
          },
          WALUpdate3(stock_t, s_remote_cnt, s_order_cnt, s_ytd));
   }

   // o_ol_cnt
   oBuffer.push8(lineNumbers.size());

   Numeric total_amount = 0;
   for (unsigned i = 0; i < lineNumbers.size(); i++) {
      Integer lineNumber = lineNumbers[i];
      Integer supware = supwares[i];
      Integer itemid = itemids[i];
      Numeric qty = qtys[i];

      Numeric i_price;  // TODO: rollback on miss
      rp = oBuffer.setResetPoint();
      item.lookup1({itemid}, [&](const item_t& rec) {
         oBuffer.reset(rp);
         i_price = rec.i_price;
         oBuffer.pushVC(rec.i_name);
         oBuffer.pushD(i_price);
      });
      oBuffer.dropResetPoint(rp);

      Varchar<24> s_dist;
      rp = oBuffer.setResetPoint();
      stock.lookup1({w_id, itemid}, [&](const stock_t& rec) {
         oBuffer.reset(rp);
         oBuffer.pushD(rec.s_quantity);
         switch (d_id) {
            case 1:
               s_dist = rec.s_dist_01;
               break;
            case 2:
               s_dist = rec.s_dist_02;
               break;
            case 3:
               s_dist = rec.s_dist_03;
               break;
            case 4:
               s_dist = rec.s_dist_04;
               break;
            case 5:
               s_dist = rec.s_dist_05;
               break;
            case 6:
               s_dist = rec.s_dist_06;
               break;
            case 7:
               s_dist = rec.s_dist_07;
               break;
            case 8:
               s_dist = rec.s_dist_08;
               break;
            case 9:
               s_dist = rec.s_dist_09;
               break;
            case 10:
               s_dist = rec.s_dist_10;
               break;
            default:
               exit(1);
               throw;
         }
      });
      oBuffer.dropResetPoint(rp);
      Numeric ol_amount = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
      oBuffer.pushD(ol_amount);
      total_amount += ol_amount;
      Timestamp ol_delivery_d = 0;  // NULL
      orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
      // TODO: i_data, s_data
   }

   oBuffer.pushD(total_amount);
}

void delivery(Integer w_id, Integer carrier_id, Timestamp datetime)
{
   for (Integer d_id = 1; d_id <= 10; d_id++) {
      Integer o_id = minInteger;
      neworder.scan(
          {w_id, d_id, minInteger},
          [&](const neworder_t::Key& key, const neworder_t&) {
             if (key.no_w_id == w_id && key.no_d_id == d_id) {
                o_id = key.no_o_id;
             }
             return false;
          },
          [&]() { o_id = minInteger; });
      if (o_id == minInteger)
         continue;
      // -------------------------------------------------------------------------------------
      if (FLAGS_tpcc_remove) {
         const auto ret = neworder.erase({w_id, d_id, o_id});
         ensure(!FLAGS_si || ret);
      }
      // -------------------------------------------------------------------------------------
      // Integer ol_cnt = minInteger, c_id;
      // order.scan({w_id, d_id, o_id}, [&](const order_t& rec) { ol_cnt = rec.o_ol_cnt; c_id = rec.o_c_id; return false; });
      // if (ol_cnt == minInteger)
      // continue;
      Integer ol_cnt, c_id;

      bool is_safe_to_continue = false;
      order.scan(
          {w_id, d_id, o_id},
          [&](const order_t::Key& key, const order_t& rec) {
             if (key.o_w_id == w_id && key.o_d_id == d_id && key.o_id == o_id) {
                is_safe_to_continue = true;
                ol_cnt = rec.o_ol_cnt;
                c_id = rec.o_c_id;
             } else {
                is_safe_to_continue = false;
             }
             return false;
          },
          [&]() { is_safe_to_continue = false; });
      if (!is_safe_to_continue)
         continue;
      order.update1(
          {w_id, d_id, o_id}, [&](order_t& rec) { rec.o_carrier_id = carrier_id; }, WALUpdate1(order_t, o_carrier_id));
      // -------------------------------------------------------------------------------------
      // First check if all orderlines have been inserted, a hack because of the missing transaction and concurrency control
      orderline.scan(
          {w_id, d_id, o_id, ol_cnt},
          [&](const orderline_t::Key& key, const orderline_t&) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id && key.ol_number == ol_cnt) {
                is_safe_to_continue = true;
             } else {
                is_safe_to_continue = false;
             }
             return false;
          },
          [&]() { is_safe_to_continue = false; });
      if (!is_safe_to_continue) {
         continue;
      }
      // -------------------------------------------------------------------------------------
      Numeric ol_total = 0;
      for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++) {
         orderline.update1(
             {w_id, d_id, o_id, ol_number},
             [&](orderline_t& rec) {
                ol_total += rec.ol_amount;
                rec.ol_delivery_d = datetime;
             },
             WALUpdate1(orderline_t, ol_delivery_d));
      }

      customer.update1(
          {w_id, d_id, c_id},
          [&](customer_t& rec) {
             rec.c_balance += ol_total;
             rec.c_delivery_cnt++;
          },
          WALUpdate2(customer_t, c_balance, c_delivery_cnt));
   }
}

void stockLevel(Integer w_id, Integer d_id, Integer threshold, Net::OBuffer<uint8_t>& oBuffer)
{
   Integer o_id = district.lookupField({w_id, d_id}, &district_t::d_next_o_id);

   //"SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM orderline, stock WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND
   // S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"

   /*
    * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf P 116
    * EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
 FROM order_line, stock
 WHERE ol_w_id=:w_id AND
 ol_d_id=:d_id AND ol_o_id<:o_id AND
 ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
 s_i_id=ol_i_id AND s_quantity < :threshold;
    */
   vector<Integer> items;
   items.reserve(100);
   Integer min_ol_o_id = o_id - 20;
   orderline.scan(
       {w_id, d_id, min_ol_o_id, minInteger},
       [&](const orderline_t::Key& key, const orderline_t& rec) {
          if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id < o_id && key.ol_o_id >= min_ol_o_id) {
             items.push_back(rec.ol_i_id);
             return true;
          }
          return false;
       },
       [&]() { items.clear(); });
   std::sort(items.begin(), items.end());
   std::unique(items.begin(), items.end());
   uint32_t count = 0;
   for (Integer i_id : items) {
      auto res_s_quantity = stock.lookupField({w_id, i_id}, &stock_t::s_quantity);
      count += res_s_quantity < threshold;
   }
   oBuffer.push32(count);
}

void orderStatusId(Integer w_id, Integer d_id, Integer c_id, Net::OBuffer<uint8_t>& oBuffer)
{
   auto rp = oBuffer.setResetPoint();
   customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {
      oBuffer.reset(rp);
      oBuffer.pushVC(rec.c_first);
      oBuffer.pushVC(rec.c_middle);
      oBuffer.pushVC(rec.c_last);
      oBuffer.pushD(rec.c_balance);
   });
   oBuffer.dropResetPoint(rp);

   Integer o_id = -1;
   // -------------------------------------------------------------------------------------
   // latest order id desc
   if (FLAGS_order_wdc_index) {
      order_wdc.scanDesc(
          {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
          [&](const order_wdc_t::Key& key, const order_wdc_t&) {
             assert(key.o_w_id == w_id);
             assert(key.o_d_id == d_id);
             assert(key.o_c_id == c_id);
             o_id = key.o_id;
             return false;
          },
          [] {});
   } else {
      order.scanDesc(
          {w_id, d_id, std::numeric_limits<Integer>::max()},
          [&](const order_t::Key& key, const order_t& rec) {
             if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
                o_id = key.o_id;
                return false;
             }
             return true;
          },
          [&]() {});
   }
   ensure(o_id > -1);
   // -------------------------------------------------------------------------------------
   uint8_t o_ol_cnt;
   rp = oBuffer.setResetPoint();
   order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
      oBuffer.reset(rp);
      oBuffer.push64(rec.o_entry_d);
      oBuffer.push32(rec.o_carrier_id);
      o_ol_cnt = rec.o_ol_cnt;
   });
   oBuffer.dropResetPoint(rp);
   oBuffer.push8(o_ol_cnt);

   uint8_t ol_cnt;
   rp = oBuffer.setResetPoint();
   while (true) {
      ol_cnt = 0;
      // AAA: expensive
      orderline.scan(
          {w_id, d_id, o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                oBuffer.push32(rec.ol_supply_w_id);
                oBuffer.push32(rec.ol_i_id);
                oBuffer.pushD(rec.ol_quantity);
                oBuffer.pushD(rec.ol_amount);
                oBuffer.push64(rec.ol_delivery_d);
                ol_cnt++;
                return true;
             }
             return false;
          },
          [&]() {
             // NOTHING
          });
      // check if all orderlines have been inserted. retry if not
      if (ol_cnt == o_ol_cnt) {
         break;
      } else {
         oBuffer.reset(rp);
      }
   }
   oBuffer.dropResetPoint(rp);
}

void orderStatusName(Integer w_id, Integer d_id, Varchar<16> c_last, Net::OBuffer<uint8_t>& oBuffer, size_t resetIdx)
{
   vector<Integer> ids;
   customerwdl.scan(
       {w_id, d_id, c_last, {}},
       [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
          if (key.c_w_id == w_id && key.c_d_id == d_id && key.c_last == c_last) {
             ids.push_back(rec.c_id);
             return true;
          }
          return false;
       },
       [&]() { ids.clear(); });
   unsigned c_count = ids.size();
   if (c_count == 0) {
      // c_last not found
      oBuffer.reset(resetIdx);
      oBuffer.push8(static_cast<uint8_t>(TransactionType::error));
      return;
   }
   unsigned index = c_count / 2;
   if ((c_count % 2) == 0)
      index -= 1;
   Integer c_id = ids[index];

   oBuffer.push32(c_id);
   auto rp = oBuffer.setResetPoint();
   customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {
      oBuffer.reset(rp);
      oBuffer.pushVC(rec.c_first);
      oBuffer.pushVC(rec.c_middle);
      oBuffer.pushD(rec.c_balance);
   });
   oBuffer.dropResetPoint(rp);

   Integer o_id = -1;
   // latest order id desc
   if (FLAGS_order_wdc_index) {
      order_wdc.scanDesc(
          {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
          [&](const order_wdc_t::Key& key, const order_wdc_t&) {
             assert(key.o_w_id == w_id);
             assert(key.o_d_id == d_id);
             assert(key.o_c_id == c_id);
             o_id = key.o_id;
             return false;
          },
          [] {});
   } else {
      order.scanDesc(
          {w_id, d_id, std::numeric_limits<Integer>::max()},
          [&](const order_t::Key& key, const order_t& rec) {
             if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
                o_id = key.o_id;
                return false;
             }
             return true;
          },
          [&]() {});
      ensure(o_id > -1);
   }
   // -------------------------------------------------------------------------------------
   Numeric o_ol_cnt;

   rp = oBuffer.setResetPoint();
   order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
      oBuffer.reset(rp);
      oBuffer.push64(rec.o_entry_d);
      oBuffer.push32(rec.o_carrier_id);
      o_ol_cnt = rec.o_ol_cnt;
   });
   oBuffer.dropResetPoint(rp);
   oBuffer.pushD(o_ol_cnt);

   uint8_t ol_cnt;
   rp = oBuffer.setResetPoint();
   while (true) {
      ol_cnt = 0;
      orderline.scan(
          {w_id, d_id, o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                oBuffer.push32(rec.ol_supply_w_id);
                oBuffer.push32(rec.ol_i_id);
                oBuffer.pushD(rec.ol_quantity);
                oBuffer.pushD(rec.ol_amount);
                oBuffer.push64(rec.ol_delivery_d);
                ol_cnt++;
                return true;
             }
             return false;
          },
          []() {
             // NOTHING
             // TODO rollback oBuffer
          });
      // check if all orderlines have been inserted. retry if not
      if (ol_cnt == o_ol_cnt) {
         break;
      } else {
         oBuffer.reset(rp);
      }
   }
   oBuffer.dropResetPoint(rp);
}

void paymentById(Integer w_id,
                 Integer d_id,
                 Integer c_w_id,
                 Integer c_d_id,
                 Integer c_id,
                 Timestamp h_date,
                 Numeric h_amount,
                 Timestamp datetime,
                 Net::OBuffer<uint8_t>& oBuffer)
{
   Varchar<10> w_name;
   auto rp = oBuffer.setResetPoint();
   warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
      oBuffer.reset(rp);
      w_name = rec.w_name;
      oBuffer.pushVC(rec.w_street_1);
      oBuffer.pushVC(rec.w_street_2);
      oBuffer.pushVC(rec.w_city);
      oBuffer.pushVC(rec.w_state);
      oBuffer.pushVC(rec.w_zip);
   });
   oBuffer.dropResetPoint(rp);
   warehouse.update1(
       {w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, WALUpdate1(warehouse_t, w_ytd));

   Varchar<10> d_name;
   rp = oBuffer.setResetPoint();
   district.lookup1({w_id, d_id}, [&](const district_t& rec) {
      oBuffer.reset(rp);
      d_name = rec.d_name;
      oBuffer.pushVC(rec.d_street_1);
      oBuffer.pushVC(rec.d_street_2);
      oBuffer.pushVC(rec.d_city);
      oBuffer.pushVC(rec.d_state);
      oBuffer.pushVC(rec.d_zip);
   });
   oBuffer.dropResetPoint(rp);
   district.update1(
       {w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, WALUpdate1(district_t, d_ytd));

   Varchar<500> c_data;
   Varchar<2> c_credit;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   rp = oBuffer.setResetPoint();
   customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
      oBuffer.reset(rp);
      c_data = rec.c_data;
      c_credit = rec.c_credit;
      c_balance = rec.c_balance;
      c_ytd_payment = rec.c_ytd_payment;
      c_payment_cnt = rec.c_payment_cnt;
      oBuffer.pushVC(rec.c_first);
      oBuffer.pushVC(rec.c_middle);
      oBuffer.pushVC(rec.c_last);
      oBuffer.pushVC(rec.c_street_1);
      oBuffer.pushVC(rec.c_street_2);
      oBuffer.pushVC(rec.c_city);
      oBuffer.pushVC(rec.c_state);
      oBuffer.pushVC(rec.c_zip);
      oBuffer.pushVC(rec.c_phone);
      oBuffer.push64(rec.c_since);
      oBuffer.pushVC(rec.c_credit);
      oBuffer.pushD(rec.c_credit_lim);
      oBuffer.pushD(rec.c_discount);
   });
   oBuffer.dropResetPoint(rp);

   Numeric c_new_balance = c_balance - h_amount;
   Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
   Numeric c_new_payment_cnt = c_payment_cnt + 1;
   oBuffer.pushD(c_new_balance);

   if (c_credit == "BC") {
      Varchar<500> c_new_data;
      auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount, h_date,
                               w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
      c_new_data.length = numChars;
      if (c_new_data.length > 500)
         c_new_data.length = 500;
      customer.update1(
          {c_w_id, c_d_id, c_id},
          [&](customer_t& rec) {
             rec.c_data = c_new_data;
             rec.c_balance = c_new_balance;
             rec.c_ytd_payment = c_new_ytd_payment;
             rec.c_payment_cnt = c_new_payment_cnt;
          },
          WALUpdate4(customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt));

      auto c_new_data_length = c_new_data.length > 200 ? 200 : c_new_data.length;
      oBuffer.push8(c_new_data_length);
      oBuffer.push(c_new_data.data, c_new_data.data + c_new_data_length);
   } else {
      customer.update1(
          {c_w_id, c_d_id, c_id},
          [&](customer_t& rec) {
             rec.c_balance = c_new_balance;
             rec.c_ytd_payment = c_new_ytd_payment;
             rec.c_payment_cnt = c_new_payment_cnt;
          },
          WALUpdate3(customer_t, c_balance, c_ytd_payment, c_payment_cnt));

      // don't return c_data, if c_credit != "BC"
      oBuffer.push8(0);
   }

   Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
   Integer t_id = (Integer)WorkerCounters::myCounters().t_id.load();
   Integer h_id = (Integer)WorkerCounters::myCounters().variable_for_workload++;
   history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
}

void paymentByName(Integer w_id,
                   Integer d_id,
                   Integer c_w_id,
                   Integer c_d_id,
                   Varchar<16> c_last,
                   Timestamp h_date,
                   Numeric h_amount,
                   Timestamp datetime,
                   Net::OBuffer<uint8_t>& oBuffer,
                   size_t resetIdx)
{
   Varchar<10> w_name;
   warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
      w_name = rec.w_name;
      oBuffer.pushVC(rec.w_street_1);
      oBuffer.pushVC(rec.w_street_2);
      oBuffer.pushVC(rec.w_city);
      oBuffer.pushVC(rec.w_state);
      oBuffer.pushVC(rec.w_zip);
   });

   warehouse.update1(
       {w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, WALUpdate1(warehouse_t, w_ytd));
   Varchar<10> d_name;
   district.lookup1({w_id, d_id}, [&](const district_t& rec) {
      d_name = rec.d_name;
      oBuffer.pushVC(rec.d_street_1);
      oBuffer.pushVC(rec.d_street_2);
      oBuffer.pushVC(rec.d_city);
      oBuffer.pushVC(rec.d_state);
      oBuffer.pushVC(rec.d_zip);
   });
   district.update1(
       {w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, WALUpdate1(district_t, d_ytd));

   // Get customer id by name
   vector<Integer> ids;
   customerwdl.scan(
       {c_w_id, c_d_id, c_last, {}},
       [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
          if (key.c_w_id == c_w_id && key.c_d_id == c_d_id && key.c_last == c_last) {
             ids.push_back(rec.c_id);
             return true;
          }
          return false;
       },
       [&]() { ids.clear(); });
   unsigned c_count = ids.size();
   if (c_count == 0) {
      // c_last not found
      oBuffer.reset(resetIdx);
      oBuffer.push8(static_cast<uint8_t>(TransactionType::error));
      // TODO: rollback
      return;
   }
   unsigned index = c_count / 2;
   if ((c_count % 2) == 0)
      index -= 1;
   Integer c_id = ids[index];
   oBuffer.push32(c_id);

   Varchar<500> c_data;
   Varchar<2> c_credit;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   auto rp = oBuffer.setResetPoint();
   customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
      oBuffer.reset(rp);
      c_data = rec.c_data;
      c_credit = rec.c_credit;
      c_balance = rec.c_balance;
      c_ytd_payment = rec.c_ytd_payment;
      c_payment_cnt = rec.c_payment_cnt;
      oBuffer.pushVC(rec.c_first);
      oBuffer.pushVC(rec.c_middle);
      // NOTE: not returning oBuffer.pushVC(rec.c_last);
      oBuffer.pushVC(rec.c_street_1);
      oBuffer.pushVC(rec.c_street_2);
      oBuffer.pushVC(rec.c_city);
      oBuffer.pushVC(rec.c_state);
      oBuffer.pushVC(rec.c_zip);
      oBuffer.pushVC(rec.c_phone);
      oBuffer.push64(rec.c_since);
      oBuffer.pushVC(rec.c_credit);
      oBuffer.pushD(rec.c_credit_lim);
      oBuffer.pushD(rec.c_discount);
   });
   oBuffer.dropResetPoint(rp);
   Numeric c_new_balance = c_balance - h_amount;
   Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
   Numeric c_new_payment_cnt = c_payment_cnt + 1;
   oBuffer.pushD(c_new_balance);

   if (c_credit == "BC") {
      Varchar<500> c_new_data;
      auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount, h_date,
                               w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
      c_new_data.length = numChars;
      if (c_new_data.length > 500)
         c_new_data.length = 500;
      customer.update1(
          {c_w_id, c_d_id, c_id},
          [&](customer_t& rec) {
             rec.c_data = c_new_data;
             rec.c_balance = c_new_balance;
             rec.c_ytd_payment = c_new_ytd_payment;
             rec.c_payment_cnt = c_new_payment_cnt;
          },
          WALUpdate4(customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt));

      auto c_new_data_length = c_new_data.length > 200 ? 200 : c_new_data.length;
      oBuffer.push8(c_new_data_length);
      oBuffer.push(c_new_data.data, c_new_data.data + c_new_data_length);
   } else {
      customer.update1(
          {c_w_id, c_d_id, c_id},
          [&](customer_t& rec) {
             rec.c_balance = c_new_balance;
             rec.c_ytd_payment = c_new_ytd_payment;
             rec.c_payment_cnt = c_new_payment_cnt;
          },
          WALUpdate3(customer_t, c_balance, c_ytd_payment, c_payment_cnt));

      // don't return c_data, if c_credit != "BC"
      oBuffer.push8(0);
   }

   Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
   Integer t_id = Integer(WorkerCounters::myCounters().t_id.load());
   Integer h_id = (Integer)WorkerCounters::myCounters().variable_for_workload++;
   history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
}
}  // namespace TPCC
