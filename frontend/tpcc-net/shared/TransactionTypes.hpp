#include <cstdint>

namespace TPCC
{
enum class TransactionType : uint8_t {
   notSet = 0,
   newOrder = 1,
   delivery = 2,
   stockLevel = 3,
   orderStatusId = 4,
   orderStatusName = 5,
   paymentById = 6,
   paymentByName = 7,
   aborted = 8
};
}