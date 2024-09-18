#pragma once
#include <asm-generic/errno-base.h>
#include <execinfo.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <thread>

#define ASSERT_RW(value) \
  do {                   \
    (value);             \
  } while (0)

inline ssize_t readn(int fd, void *vptr, size_t n) {
  ssize_t nread;
  char *ptr = (char *)vptr;
  size_t nleft = n;
  while (nleft > 0) {
    if ((nread = read(fd, ptr, nleft)) < 0) {
      if (errno == EINTR)
        nread = 0;
      else {
        std::cout << std::this_thread::get_id() << "read error, return "
                  << nread << ", nleft " << nleft << " , n " << n << " , fd "
                  << fd << ", Error: " << strerror(errno) << std::endl;
        const int maxStackTraceSize = 20;
        void *stackTrace[maxStackTraceSize];
        int stackTraceSize = backtrace(stackTrace, maxStackTraceSize);
        char **stackTraceSymbols =
            backtrace_symbols(stackTrace, stackTraceSize);

        std::cout << "Call stack:" << std::endl;
        for (int i = 0; i < stackTraceSize; ++i) {
          std::cout << stackTraceSymbols[i] << std::endl;
        }

        free(stackTraceSymbols);
        return -1;
      }
    }
    nleft -= nread;
    ptr += nread;
  }
  return (ssize_t)(n - nleft);
}

inline ssize_t writen(int fd, const void *vptr, size_t n) {
  ssize_t nwritten;
  const char *ptr = (const char *)vptr;
  size_t nleft = n;
  while (nleft > 0) {
    if ((nwritten = write(fd, ptr, nleft)) < 0) {
      if (nwritten < 0 && errno == EINTR)
        nwritten = 0;
      else {
        std::cout << std::this_thread::get_id() << "writen error, return "
                  << nwritten << ", nleft " << nleft << " , n " << n << " , fd "
                  << fd << ", Error: " << strerror(errno) << std::endl;
        const int maxStackTraceSize = 20;
        void *stackTrace[maxStackTraceSize];
        int stackTraceSize = backtrace(stackTrace, maxStackTraceSize);
        char **stackTraceSymbols =
            backtrace_symbols(stackTrace, stackTraceSize);
        std::cout << "Call stack:" << std::endl;
        for (int i = 0; i < stackTraceSize; ++i) {
          std::cout << stackTraceSymbols[i] << std::endl;
        }
        free(stackTraceSymbols);
        return -1;
      }
    }
    nleft -= nwritten;
    ptr += nwritten;
  }
  return (ssize_t)n;
}