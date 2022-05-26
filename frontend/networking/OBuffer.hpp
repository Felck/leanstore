#pragma once
#include <arpa/inet.h>
#include <endian.h>

#include <vector>

#include "BitCast.hpp"
#include "types.hpp"

namespace Net
{
template <typename T>
class OBuffer
{
  public:
   template <typename InputIterator>
   void push(InputIterator first, InputIterator last)
   {
      buffer.insert(buffer.end(), first, last);
   };

   void push8(uint8_t data) { buffer.push_back(data); };

   void push32(uint32_t data)
   {
      auto d = htobe32(data);
      // NOTE: technically UB
      push(reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 4);
   }

   void push64(uint64_t data)
   {
      auto d = htobe64(data);
      // NOTE: technically UB
      push(reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 8);
   }

   void pushD(double data)
   {
      auto d = bit_cast<uint64_t>(data);
      d = htobe64(data);
      // NOTE: technically UB
      push(reinterpret_cast<uint8_t*>(&d), reinterpret_cast<uint8_t*>(&d) + 8);
   }

   // TODO add throwing template and move specialisation out of this file (decouple from tpcc type varchar)
   template <int maxLength>
   void pushVC(const Varchar<maxLength>& data)
   {
      if (data.length == 0) {
         push8(0);
      } else {
         push8(data.length);
         push(data.data, data.data + data.length);
      }
   }

   ssize_t send(int fd)
   {
      if (buffer.empty())
         return 0;

      auto n = ::send(fd, &buffer[0], buffer.size(), MSG_NOSIGNAL);

      if (n > 0) {
         if (static_cast<size_t>(n) == buffer.size()) {
            buffer.clear();
         } else {
            buffer.erase(buffer.begin(), buffer.begin() + n);
         }
      }

      return n;
   };

   void pop_front(size_t n)
   {
      if (n > 0) {
         if (static_cast<size_t>(n) == buffer.size()) {
            buffer.clear();
         } else {
            buffer.erase(buffer.begin(), buffer.begin() + n);
         }
      }
   }

   bool empty() { return buffer.empty(); }

   size_t setResetPoint()
   {
      resetIdxs.push_back(buffer.size());
      return resetIdxs.size() - 1;
   };

   void dropResetPoint(size_t i) { resetIdxs.erase(resetIdxs.begin() + i, resetIdxs.end()); }

   void reset(size_t i) { buffer.erase(buffer.begin() + resetIdxs[i], buffer.end()); };

   void resetAndDrop(size_t i)
   {
      reset(i);
      dropResetPoint(i);
   }

   // private:
   std::vector<T> buffer;
   std::vector<size_t> resetIdxs;
};
}  // namespace Net
