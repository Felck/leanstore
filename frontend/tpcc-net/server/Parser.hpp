#pragma once
#include <endian.h>

#include <atomic>
#include <vector>

#include "OBuffer.hpp"
#include "TransactionTypes.hpp"
#include "workload.hpp"

namespace TPCC
{
union FunctionParams {
   struct NewOrder {
      uint64_t timestamp;
      uint32_t w_id;
      uint32_t d_id;
      uint32_t c_id;
      uint8_t vecSize;
   } newOrder;
   struct Delivery {
      uint64_t datetime;
      uint32_t w_id;
      uint32_t carrier_id;
   } delivery;
   struct StockLevel {
      uint32_t w_id;
      uint32_t d_id;
      uint32_t threshold;
   } stockLevel;
   struct OrderStatus {
      uint32_t w_id;
      uint32_t d_id;
      uint32_t c_id;
   } orderStatusId;
   struct OrderStatusName {
      uint32_t w_id;
      uint32_t d_id;
      char c_last[16];  // TODO: change to Varchar<16>
      uint8_t strLength;
   } orderStatusName;
   struct PaymentById {
      uint64_t h_date;
      uint64_t h_amount;
      uint64_t datetime;
      uint32_t w_id;
      uint32_t d_id;
      uint32_t c_w_id;
      uint32_t c_d_id;
      uint32_t c_id;
   } paymentById;
   struct PaymentByName {
      uint64_t h_date;
      uint64_t h_amount;
      uint64_t datetime;
      uint32_t w_id;
      uint32_t d_id;
      uint32_t c_w_id;
      uint32_t c_d_id;
      char c_last[16];  // TODO: change to Varchar<16>
      uint8_t strLength;
   } paymentByName;
};

struct VectorParams {
   std::vector<int32_t> lineNumbers;
   std::vector<int32_t> supwares;
   std::vector<int32_t> itemids;
   std::vector<int32_t> qtys;
};

class Parser
{
  public:
   // TODO: calc exact value
   static constexpr size_t maxPacketSize = 200;

   Parser(Net::OBuffer<uint8_t>& oBuffer) : oBuffer(oBuffer) {}

   template <typename Func>
   void parse(const uint8_t* data, size_t length, Func callback)
   {
      for (size_t i = 0; i != length; i++) {
         switch (funcID) {
            case TransactionType::notSet:
               // new paket: read function ID
               funcID = static_cast<TransactionType>(data[i]);
               byteIndex = 0;
               break;
            case TransactionType::newOrder:
               parseNewOrder(data[i], callback);
               break;
            case TransactionType::delivery:
               parseDelivery(data[i], callback);
               break;
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
         }
         byteIndex++;
      }
   }

  private:
   size_t fieldIndex = 0;
   size_t vecIndex = 0;
   size_t byteIndex = 0;
   TransactionType funcID = TransactionType::notSet;
   FunctionParams params;
   VectorParams vParams;
   Net::OBuffer<uint8_t>& oBuffer;

   inline void setUpNewPaket()
   {
      fieldIndex = 0;
      byteIndex = 0;
      funcID = TransactionType::notSet;
   }

   template <typename Func>
   void parseNewOrder(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            // read vector lengths and resize
            params.newOrder.vecSize = data;
            vParams.lineNumbers.resize(data);
            vParams.supwares.resize(data);
            vParams.itemids.resize(data);
            vParams.qtys.resize(data);
            vecIndex = 0;
            fieldIndex++;
            byteIndex = 0;
            break;
         case 1:
            parse32(params.newOrder.w_id, data);
            break;
         case 2:
            parse32(params.newOrder.d_id, data);
            break;
         case 3:
            parse32(params.newOrder.c_id, data);
            break;
         case 4:
            parseVecElement(vParams.lineNumbers.at(vecIndex), data);
            break;
         case 5:
            parseVecElement(vParams.supwares.at(vecIndex), data);
            break;
         case 6:
            parseVecElement(vParams.itemids.at(vecIndex), data);
            break;
         case 7:
            parseVecElement(vParams.qtys.at(vecIndex), data);
            break;
         case 8:
            parse64AndRun(params.newOrder.timestamp, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  newOrder(params.newOrder.w_id, params.newOrder.d_id, params.newOrder.c_id, vParams.lineNumbers, vParams.supwares, vParams.itemids,
                           vParams.qtys, params.newOrder.timestamp);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::newOrder));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parseDelivery(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse32(params.delivery.w_id, data);
            break;
         case 1:
            parse32(params.delivery.carrier_id, data);
            break;
         case 2:
            parse64AndRun(params.delivery.datetime, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  delivery(params.delivery.w_id, params.delivery.carrier_id, params.delivery.datetime);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::delivery));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parseStockLevel(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse32(params.stockLevel.w_id, data);
            break;
         case 1:
            parse32(params.stockLevel.d_id, data);
            break;
         case 2:
            parse32AndRun(params.stockLevel.threshold, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  stockLevel(params.stockLevel.w_id, params.stockLevel.d_id, params.stockLevel.threshold);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::stockLevel));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parseOrderStatusId(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse32(params.orderStatusId.w_id, data);
            break;
         case 1:
            parse32(params.orderStatusId.d_id, data);
            break;
         case 2:
            parse32AndRun(params.orderStatusId.c_id, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  orderStatusId(params.orderStatusId.w_id, params.orderStatusId.d_id, params.orderStatusId.c_id);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::orderStatusId));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parseOrderStatusName(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            // read strLength and zero char array
            params.orderStatusName.strLength = data;
            std::fill(&params.orderStatusName.c_last[0], &params.orderStatusName.c_last[16], 0);
            fieldIndex++;
            byteIndex = 0;
            break;
         case 1:
            parse32(params.orderStatusName.w_id, data);
            break;
         case 2:
            parse32(params.orderStatusName.d_id, data);
            break;
         case 3:
            // read char array
            params.orderStatusName.c_last[byteIndex - 1] = data;
            if (byteIndex == params.orderStatusName.strLength) {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  orderStatusName(params.orderStatusName.w_id, params.orderStatusName.d_id, Varchar<16>(params.orderStatusName.c_last));
                  oBuffer.push(static_cast<uint8_t>(TransactionType::orderStatusName));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            }
            break;
      }
   }

   template <typename Func>
   void parsePaymentById(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            parse32(params.paymentById.w_id, data);
            break;
         case 1:
            parse32(params.paymentById.d_id, data);
            break;
         case 2:
            parse32(params.paymentById.c_w_id, data);
            break;
         case 3:
            parse32(params.paymentById.c_d_id, data);
            break;
         case 4:
            parse32(params.paymentById.c_id, data);
            break;
         case 5:
            parse64(params.paymentById.h_date, data);
            break;
         case 6:
            parse64(params.paymentById.h_amount, data);
            break;
         case 7:
            parse64AndRun(params.paymentById.datetime, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  paymentById(params.paymentById.w_id, params.paymentById.d_id, params.paymentById.c_w_id, params.paymentById.c_d_id,
                              params.paymentById.c_id, params.paymentById.h_date, params.paymentById.h_amount, params.paymentById.datetime);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::paymentById));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
               setUpNewPaket();
            });
            break;
      }
   }

   template <typename Func>
   void parsePaymentByName(uint8_t data, Func callback)
   {
      switch (fieldIndex) {
         case 0:
            // read strLength and zero char array
            params.paymentByName.strLength = data;
            std::fill(&params.paymentByName.c_last[0], &params.paymentByName.c_last[16], 0);
            fieldIndex++;
            byteIndex = 0;
            break;
         case 1:
            parse32(params.paymentByName.w_id, data);
            break;
         case 2:
            parse32(params.paymentByName.d_id, data);
            break;
         case 3:
            parse32(params.paymentByName.c_w_id, data);
            break;
         case 4:
            parse32(params.paymentByName.c_d_id, data);
            break;
         case 5:
            // read char array
            params.paymentByName.c_last[byteIndex - 1] = data;
            if (byteIndex == params.paymentByName.strLength) {
               fieldIndex++;
               byteIndex = 0;
            }
            break;
         case 6:
            parse64(params.paymentByName.h_date, data);
            break;
         case 7:
            parse64(params.paymentByName.h_amount, data);
            break;
         case 8:
            parse64AndRun(params.paymentByName.datetime, data, [&]() {
               jumpmuTry()
               {
                  cr::Worker::my().startTX();
                  oBuffer.setResetPoint();

                  paymentByName(params.paymentByName.w_id, params.paymentByName.d_id, params.paymentByName.c_w_id, params.paymentByName.c_d_id,
                                Varchar<16>(params.paymentByName.c_last), params.paymentByName.h_date, params.paymentByName.h_amount,
                                params.paymentByName.datetime);
                  oBuffer.push(static_cast<uint8_t>(TransactionType::paymentByName));

                  if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
                     cr::Worker::my().abortTX();
                     oBuffer.reset();
                  } else {
                     cr::Worker::my().commitTX();
                     callback();
                  }
               }
               jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
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

   inline void parseVecElement(int32_t& dest, uint8_t data)
   {
      parse32AndRun(*reinterpret_cast<uint32_t*>(&dest), data, [&]() {
         vecIndex++;
         byteIndex = 0;
         if (vecIndex == params.newOrder.vecSize) {
            vecIndex = 0;
            fieldIndex++;
         }
      });
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
