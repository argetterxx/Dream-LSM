#pragma once
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <utility>

namespace Noncopyable {
class Noncopyable {
 public:
  Noncopyable() = default;
  virtual ~Noncopyable() = default;

  Noncopyable(const Noncopyable &) = delete;
  Noncopyable(Noncopyable &&) = delete;
  auto operator=(const Noncopyable &) noexcept -> Noncopyable & = delete;
  auto operator=(Noncopyable &&) noexcept -> Noncopyable & = delete;
};
}  // namespace Noncopyable

namespace SingletonV2 {
template <class T>
class SingletonV2 : public Noncopyable::Noncopyable {
 protected:
  SingletonV2() = default;
  ~SingletonV2() override = default;

 public:
  static auto Instance() -> T & {
    static T instance;
    return instance;
  }
};
}  // namespace SingletonV2
#define LOG(...)
/*                                                     \
  SingletonV2::SingletonV2<LocalLogger::LocalLogger>::Instance().output( \
      std::this_thread::get_id(), __FILE__, __LINE__, __FUNCTION__,  \
      __VA_ARGS__);
*/
#define LOG_CERR(...)                                                         \
                                                                              \
  SingletonV2::SingletonV2<LocalLogger::LocalLogger>::Instance().output2cerr( \
                                                                              \
      std::this_thread::get_id(), __FILE__, __LINE__, __FUNCTION__,           \
      __VA_ARGS__);