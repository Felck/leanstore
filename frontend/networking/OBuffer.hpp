#pragma once
#include <arpa/inet.h>

#include <vector>
#include <cassert>

namespace Net
{
template <typename T>
class OBuffer
{
  public:
   void push(uint8_t data)
   {
      assert(buffer.empty());
      buffer.push_back(data);
   };

   template <typename InputIterator>
   void push(InputIterator first, InputIterator last)
   {
      buffer.insert(buffer.end(), first, last);
   };

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

   void setResetPoint()
   {
      resetIdx = buffer.size();
   }

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
