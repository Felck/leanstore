#pragma once

#include "BitCast.hpp"
#include "OBuffer.hpp"
#include "TransactionTypes.hpp"
#include "workload.hpp"

namespace TPCC
{
class Parser
{
  public:
   static constexpr size_t maxPacketSize = 2000;
   static constexpr size_t alignment = 64;
   alignas(alignment) char readBuffer[maxPacketSize];

   Parser(Net::OBuffer<uint8_t>& oBuffer) : oBuffer(oBuffer) {}

   char* getReadBuffer() { return &readBuffer[packetSize]; }

   template <typename Func>
   void parse(size_t n, Func callback)
   {
      length += n;

      if (packetSize == 0 && length >= 2) {
         packetSize = (static_cast<uint8_t>(readBuffer[0]) << 8) + static_cast<uint8_t>(readBuffer[1]);
      }

      if (length == packetSize) {
         switch (static_cast<TransactionType>(readBuffer[2])) {
            case TransactionType::newOrder:
               parseNewOrder(callback);
               break;
            case TransactionType::delivery:
               parseDelivery(callback);
               break;
            case TransactionType::stockLevel:
               parseStockLevel(callback);
               break;
            case TransactionType::orderStatusId:
               parseOrderStatusId(callback);
               break;
            case TransactionType::orderStatusName:
               parseOrderStatusName(callback);
               break;
            case TransactionType::paymentById:
               parsePaymentById(callback);
               break;
            case TransactionType::paymentByName:
               parsePaymentByName(callback);
               break;
            default:
               throw "invalid transaction type";
         }

         length = 0;
         packetSize = 0;
      }
   }

  private:
   size_t length = 0;
   size_t packetSize = 0;
   Net::OBuffer<uint8_t>& oBuffer;

   template <typename Func>
   void parseNewOrder(Func callback)
   {
      size_t vecLength = readBuffer[3];
      std::vector<Integer> lineNumbers(vecLength);
      std::vector<Integer> supwares(vecLength);
      std::vector<Integer> itemIds(vecLength);
      std::vector<Integer> qtys(vecLength);
      size_t cpyIndex = 28;
      std::memcpy(&lineNumbers[0], &readBuffer[cpyIndex], vecLength * sizeof(Integer));
      cpyIndex += vecLength * sizeof(Integer);
      std::memcpy(&supwares[0], &readBuffer[cpyIndex], vecLength * sizeof(Integer));
      cpyIndex += vecLength * sizeof(Integer);
      std::memcpy(&itemIds[0], &readBuffer[cpyIndex], vecLength * sizeof(Integer));
      cpyIndex += vecLength * sizeof(Integer);
      std::memcpy(&qtys[0], &readBuffer[cpyIndex], vecLength * sizeof(Integer));

      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::newOrder));
         newOrder(bit_cast<Integer>(&readBuffer[16]), bit_cast<Integer>(&readBuffer[20]), bit_cast<Integer>(&readBuffer[24]), lineNumbers, supwares,
                  itemIds, qtys, bit_cast<Timestamp>(&readBuffer[8]), oBuffer);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
            // TODO push data for rolled back new order transaction to oBuffer
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parseDelivery(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::delivery));
         delivery(bit_cast<Timestamp>(&readBuffer[16]), bit_cast<Integer>(&readBuffer[20]), bit_cast<Integer>(&readBuffer[8]));

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parseStockLevel(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::stockLevel));
         stockLevel(bit_cast<Integer>(&readBuffer[8]), bit_cast<Integer>(&readBuffer[12]), bit_cast<Integer>(&readBuffer[16]), oBuffer);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parseOrderStatusId(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::orderStatusId));
         orderStatusId(bit_cast<Integer>(&readBuffer[8]), bit_cast<Integer>(&readBuffer[12]), bit_cast<Integer>(&readBuffer[16]), oBuffer);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parseOrderStatusName(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::orderStatusName));
         orderStatusName(bit_cast<Integer>(&readBuffer[8]), bit_cast<Integer>(&readBuffer[12]), Varchar<16>(&readBuffer[12], readBuffer[3]), oBuffer,
                         i);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parsePaymentById(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::paymentById));
         paymentById(bit_cast<Integer>(&readBuffer[32]), bit_cast<Integer>(&readBuffer[36]), bit_cast<Integer>(&readBuffer[40]),
                     bit_cast<Integer>(&readBuffer[44]), bit_cast<Integer>(&readBuffer[48]), bit_cast<Timestamp>(&readBuffer[8]),
                     bit_cast<double>(&readBuffer[16]), bit_cast<Timestamp>(&readBuffer[24]), oBuffer);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }

   template <typename Func>
   void parsePaymentByName(Func callback)
   {
      jumpmuTry()
      {
         cr::Worker::my().startTX();
         size_t i = oBuffer.setResetPoint();

         oBuffer.push8(static_cast<uint8_t>(TransactionType::paymentByName));
         paymentByName(bit_cast<Integer>(&readBuffer[32]), bit_cast<Integer>(&readBuffer[36]), bit_cast<Integer>(&readBuffer[40]),
                       bit_cast<Integer>(&readBuffer[44]), Varchar<16>(&readBuffer[48], readBuffer[3]), bit_cast<Timestamp>(&readBuffer[8]),
                       bit_cast<double>(&readBuffer[16]), bit_cast<Timestamp>(&readBuffer[24]), oBuffer, i);

         if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
            cr::Worker::my().abortTX();
            oBuffer.resetAndDrop(i);
            oBuffer.push8(static_cast<uint8_t>(TransactionType::aborted));
         } else {
            oBuffer.dropResetPoint(i);
            cr::Worker::my().commitTX();
            callback();
         }
      }
      jumpmuCatch() { WorkerCounters::myCounters().tx_abort++; }
   }
};
}  // namespace TPCC
