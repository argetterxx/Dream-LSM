#include <cassert>
#include <cstddef>
#include <iterator>
#include <thread>

#include "db/remote_flush_job.h"
#include "env/emulated_clock.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
class SystemClockFactory {
 public:
  static void* UnPackLocal(TransferService* node);

  SystemClockFactory(const SystemClockFactory&) = delete;
  void operator=(const SystemClockFactory&) = delete;

 private:
  SystemClockFactory() = default;
  ~SystemClockFactory() = default;
};
inline void* SystemClockFactory::UnPackLocal(TransferService* node) {
  uint8_t msg = 0;
  node->receive(&msg, sizeof(msg));
  if (msg == 0) {
    return Env::Default()->GetSystemClock().get();
  } else if (msg == 1) {
    return Env::Default()->GetSystemClock().get();
  } else if (msg == 2) {
    return new EmulatedSystemClock(SystemClock::Default());
  } else {
    assert(false);
    return nullptr;
  }
}
}  // namespace ROCKSDB_NAMESPACE