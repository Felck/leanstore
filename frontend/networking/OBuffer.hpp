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

   bool empty() { return buffer.empty(); }

   void setResetPoint() { resetIdx = buffer.size(); }

   void reset()
   {
      buffer.erase(buffer.begin() + resetIdx, buffer.end());
      resetIdx = 0;
   };

  private:
   std::vector<T> buffer;
   size_t resetIdx;
};
}  // namespace Net
