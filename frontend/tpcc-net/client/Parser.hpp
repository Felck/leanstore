#pragma once
#include <endian.h>

#include <atomic>
#include <vector>

#include "BitCast.hpp"
#include "TransactionTypes.hpp"

namespace TPCC
{
// TODO: investigate performance impact of changing union to variant and char[] to Varchar
// (std::get_if shouldn't introduce much overhead over union access)
union ResponseData {
   struct NewOrder {
      uint64_t c_discount;  // double
      uint64_t w_tax;       // double
      uint64_t d_tax;       // double
      uint64_t o_entry_d;
      uint32_t o_id;
      uint64_t total_amount;  // double
      uint8_t o_ol_cnt;
      uint8_t c_last_length;
      char c_last[16];
      uint8_t c_credit_length;
      char c_credit[2];
   } newOrder;
   struct StockLevel {
      uint32_t low_stock;
   } stockLevel;
   struct OrderStatus {
      uint64_t c_balance;  // double
      uint64_t o_entry_d;
      uint32_t o_carrier_id;
      uint32_t c_id;
      uint8_t c_first_length;
      char c_first[16];
      uint8_t c_middle_length;
      char c_middle[16];
      uint8_t c_last_length;
      char c_last[16];
      uint8_t o_ol_cnt;
   } orderStatus;
   struct Payment {
      uint64_t c_since;
      uint64_t c_credit_lim;   // double
      uint64_t c_discount;     // double
      uint64_t c_new_balance;  // double
      uint32_t c_id;

      uint8_t w_street_1_length;
      char w_street_1[20];
      uint8_t w_street_2_length;
      char w_street_2[20];
      uint8_t w_city_length;
      char w_city[20];
      uint8_t w_state_length;
      char w_state[2];
      uint8_t w_zip_length;
      char w_zip[9];

      uint8_t d_street_1_length;
      char d_street_1[20];
      uint8_t d_street_2_length;
      char d_street_2[20];
      uint8_t d_city_length;
      char d_city[20];
      uint8_t d_state_length;
      char d_state[2];
      uint8_t d_zip_length;
      char d_zip[9];

      uint8_t c_first_length;
      char c_first[16];
      uint8_t c_middle_length;
      char c_middle[2];
      uint8_t c_last_length;
      char c_last[16];
      uint8_t c_street_1_length;
      char c_street_1[20];
      uint8_t c_street_2_length;
      char c_street_2[20];
      uint8_t c_city_length;
      char c_city[20];
      uint8_t c_state_length;
      char c_state[2];
      uint8_t c_zip_length;
      char c_zip[9];
      uint8_t c_phone_length;
      char c_phone[16];
      uint8_t c_credit_length;
      char c_credit[2];
      uint8_t c_new_data_length;
      char c_new_data[200];
   } payment;
};

struct ResponseDataVectors {
   uint8_t i_name_lengths[16];
   char i_names[16][24];
   uint64_t i_prices[16];       // double
   uint64_t s_quantities[16];   // double
   uint64_t ol_amounts[16];     // double
   uint64_t ol_quantities[16];  // double
   uint64_t ol_delivery_ds[16];
   uint32_t ol_supply_w_ids[16];
   uint32_t ol_i_ids[16];
};

class Parser
{
  private:
   size_t fieldIndex = 0;
   size_t vecIndex = 0;
   size_t byteIndex = 0;
   TransactionType txType = TransactionType::notSet;
   ResponseData response;
   ResponseDataVectors responseVecs;

  public:
   static constexpr size_t maxPacketSize = 2048;  // TODO calc

   template <typename Func>
   void parse(const uint8_t* data, size_t length, Func callback)
   {
      for (size_t i = 0; i != length; i++) {
         switch (txType) {
            case TransactionType::notSet:
               // new paket: read function ID
               txType = static_cast<TransactionType>(data[i]);
               if (static_cast<TransactionType>(data[i]) == TransactionType::delivery) {
                  callback();
                  setUpNewPaket();
               } else if (static_cast<TransactionType>(data[i]) == TransactionType::error) {
                  callback();
                  setUpNewPaket();
               } else {
                  byteIndex = 0;
               }
               break;
            case TransactionType::newOrder:
               parseNewOrder(data[i], callback);
               break;
            case TransactionType::delivery:
               throw "unexpected transaction type: delivery";
            case TransactionType::stockLevel:
               parseStockLevel(data[i], callback);
               break;
            case TransactionType::orderStatusId:
               parseOrderStatusId(data[i], callback);
               break;
            case TransactionType::orderStatusName:
               parseOrderStatusName(data[i], callback);
               break;
            case TransactionType::paymentById:
               parsePaymentById(data[i], callback);
               break;
            case TransactionType::paymentByName:
               parsePaymentByName(data[i], callback);
               break;
            case TransactionType::aborted:
               throw "transaction aborted";
            case TransactionType::error:
               throw "transaction error";
         }
         byteIndex++;
      }
   }

   TransactionType getTxType() const { return txType; }
   const ResponseData& getResponse() const { return response; }
   const ResponseDataVectors& getResponseVecs() const { return responseVecs; }

  private:
   inline void setUpNewPaket()
   {
      fieldIndex = 0;
      byteIndex = 0;  // TODO delete
      txType = TransactionType::notSet;
   }

   template <typename Func>
   void parseNewOrder(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse8(response.newOrder.c_last_length, data);
            break;
         case 1:
            parseStr(response.newOrder.c_last, response.newOrder.c_last_length, data);
            break;
         case 2:
            parse8(response.newOrder.c_credit_length, data);
            break;
         case 3:
            parseStr(response.newOrder.c_credit, response.newOrder.c_credit_length, data);
            break;
         case 4:
            parse64(response.newOrder.c_discount, data);
            break;
         case 5:
            parse64(response.newOrder.w_tax, data);
            break;
         case 6:
            parse64(response.newOrder.d_tax, data);
            break;
         case 7:
            parse32(response.newOrder.o_id, data);
            break;
         case 8:
            parse64(response.newOrder.o_entry_d, data);
            break;
         case 9:
            parse8(response.newOrder.o_ol_cnt, data);
            vecIndex = 0;
            break;
         case 10:
            parse8(responseVecs.i_name_lengths[vecIndex], data);
            break;
         case 11:
            parseStr(responseVecs.i_names[vecIndex], responseVecs.i_name_lengths[vecIndex], data);
            break;
         case 12:
            parse64(responseVecs.i_prices[vecIndex], data);
            break;
         case 13:
            parse64(responseVecs.s_quantities[vecIndex], data);
            break;
         case 14:
            parse64AndRun(responseVecs.ol_amounts[vecIndex], data, [&]() {
               byteIndex = 0;
               if (vecIndex + 1 != response.newOrder.o_ol_cnt) {
                  vecIndex++;
                  fieldIndex = 10;
               } else {
                  fieldIndex++;
               }
            });
            break;
         case 15:
            parse64AndRun(response.newOrder.total_amount, data, [&]() {
               callback();
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parseStockLevel(uint8_t data, Func callback)
   {
      parse32AndRun(response.stockLevel.low_stock, data, [&]() {
         callback();
         setUpNewPaket();
      });
   }

   template <typename Func>
   void parseOrderStatusId(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse8(response.orderStatus.c_first_length, data);
            break;
         case 1:
            parseStr(response.orderStatus.c_first, response.orderStatus.c_first_length, data);
            break;
         case 2:
            parse8(response.orderStatus.c_middle_length, data);
            break;
         case 3:
            parseStr(response.orderStatus.c_middle, response.orderStatus.c_middle_length, data);
            break;
         case 4:
            parse8(response.orderStatus.c_last_length, data);
            break;
         case 5:
            parseStr(response.orderStatus.c_last, response.orderStatus.c_last_length, data);
            break;
         case 6:
            parse64(response.orderStatus.c_balance, data);
            break;
         case 7:
            parse64(response.orderStatus.o_entry_d, data);
            break;
         case 8:
            parse32(response.orderStatus.o_carrier_id, data);
            break;
         case 9:
            parse8(response.orderStatus.o_ol_cnt, data);
            vecIndex = 0;
            break;
         case 10:
            parse32(responseVecs.ol_supply_w_ids[vecIndex], data);
            break;
         case 11:
            parse32(responseVecs.ol_i_ids[vecIndex], data);
            break;
         case 12:
            parse64(responseVecs.ol_quantities[vecIndex], data);
            break;
         case 13:
            parse64(responseVecs.ol_amounts[vecIndex], data);
            break;
         case 14:
            parse64AndRun(responseVecs.ol_delivery_ds[vecIndex], data, [&]() {
               byteIndex = 0;
               if (vecIndex + 1 != response.orderStatus.o_ol_cnt) {
                  vecIndex++;
                  fieldIndex = 10;
               } else {
                  callback();
                  setUpNewPaket();
               }
            });
            break;
      }
   }

   template <typename Func>
   void parseOrderStatusName(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse32(response.orderStatus.c_id, data);
            break;
         case 1:
            parse8(response.orderStatus.c_first_length, data);
            break;
         case 2:
            parseStr(response.orderStatus.c_first, response.orderStatus.c_first_length, data);
            break;
         case 3:
            parse8(response.orderStatus.c_middle_length, data);
            break;
         case 4:
            parseStr(response.orderStatus.c_middle, response.orderStatus.c_middle_length, data);
            break;
         case 5:
            parse64(response.orderStatus.c_balance, data);
            break;
         case 6:
            parse64(response.orderStatus.o_entry_d, data);
            break;
         case 7:
            parse32(response.orderStatus.o_carrier_id, data);
            break;
         case 8:
            parse8(response.orderStatus.o_ol_cnt, data);
            vecIndex = 0;
            break;
         case 9:
            parse32(responseVecs.ol_supply_w_ids[vecIndex], data);
            break;
         case 10:
            parse32(responseVecs.ol_i_ids[vecIndex], data);
            break;
         case 11:
            parse64(responseVecs.ol_quantities[vecIndex], data);
            break;
         case 12:
            parse64(responseVecs.ol_amounts[vecIndex], data);
            break;
         case 13:
            parse64AndRun(responseVecs.ol_delivery_ds[vecIndex], data, [&]() {
               byteIndex = 0;
               if (vecIndex + 1 != response.orderStatus.o_ol_cnt) {
                  vecIndex++;
                  fieldIndex = 9;
               } else {
                  callback();
                  setUpNewPaket();
               }
            });
            break;
      }
   }

   template <typename Func>
   void parsePaymentById(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         // warehouse
         case 0:
            parse8(response.payment.w_street_1_length, data);
            break;
         case 1:
            parseStr(response.payment.w_street_1, response.payment.w_street_1_length, data);
            break;
         case 2:
            parse8(response.payment.w_street_2_length, data);
            break;
         case 3:
            parseStr(response.payment.w_street_2, response.payment.w_street_2_length, data);
            break;
         case 4:
            parse8(response.payment.w_city_length, data);
            break;
         case 5:
            parseStr(response.payment.w_city, response.payment.w_city_length, data);
            break;
         case 6:
            parse8(response.payment.w_state_length, data);
            break;
         case 7:
            parseStr(response.payment.w_state, response.payment.w_state_length, data);
            break;
         case 8:
            parse8(response.payment.w_zip_length, data);
            break;
         case 9:
            parseStr(response.payment.w_zip, response.payment.w_zip_length, data);
            break;
         // district
         case 10:
            parse8(response.payment.d_street_1_length, data);
            break;
         case 11:
            parseStr(response.payment.d_street_1, response.payment.d_street_1_length, data);
            break;
         case 12:
            parse8(response.payment.d_street_2_length, data);
            break;
         case 13:
            parseStr(response.payment.d_street_2, response.payment.d_street_2_length, data);
            break;
         case 14:
            parse8(response.payment.d_city_length, data);
            break;
         case 15:
            parseStr(response.payment.d_city, response.payment.d_city_length, data);
            break;
         case 16:
            parse8(response.payment.d_state_length, data);
            break;
         case 17:
            parseStr(response.payment.d_state, response.payment.d_state_length, data);
            break;
         case 18:
            parse8(response.payment.d_zip_length, data);
            break;
         case 19:
            parseStr(response.payment.d_zip, response.payment.d_zip_length, data);
            break;
         // customer
         case 20:
            parse8(response.payment.c_first_length, data);
            break;
         case 21:
            parseStr(response.payment.c_first, response.payment.c_first_length, data);
            break;
         case 22:
            parse8(response.payment.c_middle_length, data);
            break;
         case 23:
            parseStr(response.payment.c_middle, response.payment.c_middle_length, data);
            break;
         case 24:
            parse8(response.payment.c_last_length, data);
            break;
         case 25:
            parseStr(response.payment.c_last, response.payment.c_last_length, data);
            break;
         case 26:
            parse8(response.payment.c_street_1_length, data);
            break;
         case 27:
            parseStr(response.payment.c_street_1, response.payment.c_street_1_length, data);
            break;
         case 28:
            parse8(response.payment.c_street_2_length, data);
            break;
         case 29:
            parseStr(response.payment.c_street_2, response.payment.c_street_2_length, data);
            break;
         case 30:
            parse8(response.payment.c_city_length, data);
            break;
         case 31:
            parseStr(response.payment.c_city, response.payment.c_city_length, data);
            break;
         case 32:
            parse8(response.payment.c_state_length, data);
            break;
         case 33:
            parseStr(response.payment.c_state, response.payment.c_state_length, data);
            break;
         case 34:
            parse8(response.payment.c_zip_length, data);
            break;
         case 35:
            parseStr(response.payment.c_zip, response.payment.c_zip_length, data);
            break;
         case 36:
            parse8(response.payment.c_phone_length, data);
            break;
         case 37:
            parseStr(response.payment.c_phone, response.payment.c_phone_length, data);
            break;
         case 38:
            parse64(response.payment.c_since, data);
            break;
         case 39:
            parse8(response.payment.c_credit_length, data);
            break;
         case 40:
            parseStr(response.payment.c_credit, response.payment.c_credit_length, data);
            break;
         case 41:
            parse64(response.payment.c_credit_lim, data);
            break;
         case 42:
            parse64(response.payment.c_discount, data);
            break;
         case 43:
            parse64(response.payment.c_new_balance, data);
            break;
         case 44:
            parse8(response.payment.c_new_data_length, data);
            if (response.payment.c_new_data_length == 0) {
               callback();
               setUpNewPaket();
            }
            break;
         case 45:
            parseStrAndRun(response.payment.c_new_data, response.payment.c_new_data_length, data, [&]() {
               callback();
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parsePaymentByName(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         // warehouse
         case 0:
            parse8(response.payment.w_street_1_length, data);
            break;
         case 1:
            parseStr(response.payment.w_street_1, response.payment.w_street_1_length, data);
            break;
         case 2:
            parse8(response.payment.w_street_2_length, data);
            break;
         case 3:
            parseStr(response.payment.w_street_2, response.payment.w_street_2_length, data);
            break;
         case 4:
            parse8(response.payment.w_city_length, data);
            break;
         case 5:
            parseStr(response.payment.w_city, response.payment.w_city_length, data);
            break;
         case 6:
            parse8(response.payment.w_state_length, data);
            break;
         case 7:
            parseStr(response.payment.w_state, response.payment.w_state_length, data);
            break;
         case 8:
            parse8(response.payment.w_zip_length, data);
            break;
         case 9:
            parseStr(response.payment.w_zip, response.payment.w_zip_length, data);
            break;
         // district
         case 10:
            parse8(response.payment.d_street_1_length, data);
            break;
         case 11:
            parseStr(response.payment.d_street_1, response.payment.d_street_1_length, data);
            break;
         case 12:
            parse8(response.payment.d_street_2_length, data);
            break;
         case 13:
            parseStr(response.payment.d_street_2, response.payment.d_street_2_length, data);
            break;
         case 14:
            parse8(response.payment.d_city_length, data);
            break;
         case 15:
            parseStr(response.payment.d_city, response.payment.d_city_length, data);
            break;
         case 16:
            parse8(response.payment.d_state_length, data);
            break;
         case 17:
            parseStr(response.payment.d_state, response.payment.d_state_length, data);
            break;
         case 18:
            parse8(response.payment.d_zip_length, data);
            break;
         case 19:
            parseStr(response.payment.d_zip, response.payment.d_zip_length, data);
            break;
         // customer
         case 20:
            parse32(response.payment.c_id, data);
            break;
         case 21:
            parse8(response.payment.c_first_length, data);
            break;
         case 22:
            parseStr(response.payment.c_first, response.payment.c_first_length, data);
            break;
         case 23:
            parse8(response.payment.c_middle_length, data);
            break;
         case 24:
            parseStr(response.payment.c_middle, response.payment.c_middle_length, data);
            break;
         case 25:
            parse8(response.payment.c_street_1_length, data);
            break;
         case 26:
            parseStr(response.payment.c_street_1, response.payment.c_street_1_length, data);
            break;
         case 27:
            parse8(response.payment.c_street_2_length, data);
            break;
         case 28:
            parseStr(response.payment.c_street_2, response.payment.c_street_2_length, data);
            break;
         case 29:
            parse8(response.payment.c_city_length, data);
            break;
         case 30:
            parseStr(response.payment.c_city, response.payment.c_city_length, data);
            break;
         case 31:
            parse8(response.payment.c_state_length, data);
            break;
         case 32:
            parseStr(response.payment.c_state, response.payment.c_state_length, data);
            break;
         case 33:
            parse8(response.payment.c_zip_length, data);
            break;
         case 34:
            parseStr(response.payment.c_zip, response.payment.c_zip_length, data);
            break;
         case 35:
            parse8(response.payment.c_phone_length, data);
            break;
         case 36:
            parseStr(response.payment.c_phone, response.payment.c_phone_length, data);
            break;
         case 37:
            parse64(response.payment.c_since, data);
            break;
         case 38:
            parse8(response.payment.c_credit_length, data);
            break;
         case 39:
            parseStr(response.payment.c_credit, response.payment.c_credit_length, data);
            break;
         case 40:
            parse64(response.payment.c_credit_lim, data);
            break;
         case 41:
            parse64(response.payment.c_discount, data);
            break;
         case 42:
            parse64(response.payment.c_new_balance, data);
            break;
         case 43:
            parse8(response.payment.c_new_data_length, data);
            if (response.payment.c_new_data_length == 0) {
               callback();
               setUpNewPaket();
            }
            break;
         case 44:
            parseStrAndRun(response.payment.c_new_data, response.payment.c_new_data_length, data, [&]() {
               callback();
               setUpNewPaket();
            });
            break;
      }
   }

   inline void parse32(uint32_t& dest, uint8_t data)
   {
      parse32AndRun(dest, data, [&]() {
         fieldIndex++;
         byteIndex = 0;
      });
   }

   inline void parse64(uint64_t& dest, uint8_t data)
   {
      parse64AndRun(dest, data, [&]() {
         fieldIndex++;
         byteIndex = 0;
      });
   }

   inline void parseStr(char* str, uint8_t length, uint8_t data)
   {
      parseStrAndRun(str, length, data, [&]() {
         fieldIndex++;
         byteIndex = 0;
      });
   }

   template <typename Func>
   void parseStrAndRun(char* str, uint8_t length, uint8_t data, Func callback)
   {
      str[byteIndex - 1] = data;
      if (byteIndex == length) {
         callback();
      }
   }

   inline void parse8(uint8_t& dest, uint8_t data)
   {
      dest = data;
      fieldIndex++;
      byteIndex = 0;
   }

   template <typename Func>
   void parse32AndRun(uint32_t& dest, uint8_t data, Func callback)
   {
      switch (byteIndex) {
         case 1:
            dest = static_cast<uint32_t>(data) << 24;
            break;
         case 2:
            dest |= static_cast<uint32_t>(data) << 16;
            break;
         case 3:
            dest |= static_cast<uint32_t>(data) << 8;
            break;
         case 4:
            dest |= static_cast<uint32_t>(data);
            callback();
            break;
         default:
            throw "byte index out of range";
      }
   }

   template <typename Func>
   void parse64AndRun(uint64_t& dest, uint8_t data, Func callBack)
   {
      switch (byteIndex) {
         case 1:
            dest = static_cast<uint64_t>(data) << 56;
            break;
         case 2:
            dest |= static_cast<uint64_t>(data) << 48;
            break;
         case 3:
            dest |= static_cast<uint64_t>(data) << 40;
            break;
         case 4:
            dest |= static_cast<uint64_t>(data) << 32;
            break;
         case 5:
            dest |= static_cast<uint64_t>(data) << 24;
            break;
         case 6:
            dest |= static_cast<uint64_t>(data) << 16;
            break;
         case 7:
            dest |= static_cast<uint64_t>(data) << 8;
            break;
         case 8:
            dest |= static_cast<uint64_t>(data);
            callBack();
            break;
         default:
            throw "byte index out of range";
      }
   }
};
}  // namespace TPCC
