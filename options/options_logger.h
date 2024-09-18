#pragma once
#include <cstdarg>
#include <fstream>
#include <ios>
#include <string>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class OptionLogger : public Logger {
 public:
  void SetOutputFile(const std::string& file_name) { file_name_ = file_name; }
  void Logv(const InfoLogLevel log_level, const char* format,
            va_list ap) override {
    Logv(format, ap);
  }
  void Logv(const char* format, va_list ap) override {
    char buffer[1000];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    string_.append(buffer);
    string_.append("\n");
  }
  std::string string() const { return string_; }
  void clear() { string_.clear(); }
  void Flush() override {
    if (!file_name_.empty()) {
      std::ofstream out(file_name_.c_str(), std::ios_base::app);
      out << string_;
      out.close();
    }
    clear();
  }

 private:
  std::string file_name_;
  std::string string_;
};
}  // namespace ROCKSDB_NAMESPACE