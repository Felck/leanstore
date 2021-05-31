#include <vector>

#include "Serializer.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "types.hpp"

namespace TPCC
{
// TODO: set warehouse count
Integer warehouseCount = 1;
// -------------------------------------------------------------------------------------
constexpr Integer OL_I_ID_C = 7911;  // in range [0, 8191]
constexpr Integer C_ID_C = 259;      // in range [0, 1023]
// NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
// be within [65, 119]
constexpr Integer C_LAST_LOAD_C = 157;  // in range [0, 255]
constexpr Integer C_LAST_RUN_C = 223;   // in range [0, 255]
// -------------------------------------------------------------------------------------
constexpr Integer ITEMS_NO = 100000;  // 100K

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

Timestamp currentTimestamp()
{
   return 1;
}

// run
void newOrderRnd(std::vector<uint8_t>& buf, Integer w_id)
{
   Integer d_id = urand(1, 10);
   Integer c_id = getCustomerID();
   Integer ol_cnt = urand(5, 15);

   std::vector<Integer> lineNumbers;
   lineNumbers.reserve(15);
   std::vector<Integer> supwares;
   supwares.reserve(15);
   std::vector<Integer> itemids;
   itemids.reserve(15);
   std::vector<Integer> qtys;
   qtys.reserve(15);
   for (Integer i = 1; i <= ol_cnt; i++) {
      Integer supware = w_id;
      if (urand(1, 100) == 1)  // remote transaction
         supware = urandexcept(1, warehouseCount, w_id);
      Integer itemid = getItemID();
      if (false && (i == ol_cnt) && (urand(1, 100) == 1))  // invalid item => random
         itemid = 0;
      lineNumbers.push_back(i);
      supwares.push_back(supware);
      itemids.push_back(itemid);
      qtys.push_back(urand(1, 10));
   }
   serializeNewOrder(buf, w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, currentTimestamp());
}

void deliveryRnd(std::vector<uint8_t>& buf, Integer w_id)
{
   Integer carrier_id = urand(1, 10);
   serializeDelivery(buf, w_id, carrier_id, currentTimestamp());
}

void stockLevelRnd(std::vector<uint8_t>& buf, Integer w_id)
{
   serializeStockLevel(buf, w_id, urand(1, 10), urand(10, 20));
}

void orderStatusRnd(std::vector<uint8_t>& buf, Integer w_id)
{
   Integer d_id = urand(1, 10);
   if (urand(1, 100) <= 40) {
      serializeOrderStatusId(buf, w_id, d_id, getCustomerID());
   } else {
      serializeOrderStatusName(buf, w_id, d_id, genName(getNonUniformRandomLastNameForRun()));
   }
}

void paymentRnd(std::vector<uint8_t>& buf, Integer w_id)
{
   Integer d_id = urand(1, 10);
   Integer c_w_id = w_id;
   Integer c_d_id = d_id;
   if (urand(1, 100) > 85) {
      c_w_id = urandexcept(1, warehouseCount, w_id);
      c_d_id = urand(1, 10);
   }
   Numeric h_amount = randomNumeric(1.00, 5000.00);
   Timestamp h_date = currentTimestamp();

   if (urand(1, 100) <= 60) {
      serializePaymentByName(buf, w_id, d_id, c_w_id, c_d_id, genName(getNonUniformRandomLastNameForRun()), h_date, h_amount, currentTimestamp());
   } else {
      serializePaymentById(buf, w_id, d_id, c_w_id, c_d_id, getCustomerID(), h_date, h_amount, currentTimestamp());
   }
}

// was: [w_begin, w_end]
void tx(std::vector<uint8_t>& buf, Integer w_id)
{
   // micro-optimized version of weighted distribution
   int rnd = leanstore::utils::RandomGenerator::getRand(0, 10000);
   if (rnd < 4300) {
      paymentRnd(buf, w_id);
   }
   rnd -= 4300;
   if (rnd < 400) {
      orderStatusRnd(buf, w_id);
   }
   rnd -= 400;
   if (rnd < 400) {
      deliveryRnd(buf, w_id);
   }
   rnd -= 400;
   if (rnd < 400) {
      stockLevelRnd(buf, w_id);
   }
   rnd -= 400;
   newOrderRnd(buf, w_id);
}
}  // namespace TPCC
