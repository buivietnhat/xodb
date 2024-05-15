#pragma once

#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

namespace xodb {

/**
 * Use the macros below for generating exceptions.
 * They record where the exception was generated.
 */

#define NOT_IMPLEMENTED_EXCEPTION(msg) NotImplementedException(msg, __FILE__, __LINE__)
#define CATALOG_EXCEPTION(msg) CatalogException(msg, __FILE__, __LINE__)
#define ABORT_EXCEPTION(msg) AbortException(msg, __FILE__, __LINE__)
#define EXECUTION_EXCEPTION(msg) ExecutionException(msg, __FILE__, __LINE__)

/**
 * Exception types
 */
enum class ExceptionType : uint8_t {
  RESERVED,
  NOT_IMPLEMENTED,
  BINDER,
  CATALOG,
  MESSENGER,
  SETTINGS,
  EXECUTION
};

/**
 * Exception base class.
 */
class Exception : public std::runtime_error {
 public:
  /**
   * Creates a new Exception with the given parameters.
   * @param type exception type
   * @param msg exception message to be displayed
   * @param file name of the file in which the exception occurred
   * @param line line number at which the exception occurred
   */
  Exception(const ExceptionType type, const char *msg, const char *file, int line)
      : std::runtime_error(msg), type_(type), file_(file), line_(line) {}

  /**
   * Allows type and source location of the exception to be recorded in the log
   * at the catch point.
   */
  friend std::ostream &operator<<(std::ostream &out, const Exception &ex) {
    out << ex.GetType() << " exception:";
    out << ex.GetFile() << ":";
    out << ex.GetLine() << ":";
    out << ex.what();
    return out;
  }

  /**
   * @return the exception type
   */
  const char *GetType() const {
    switch (type_) {
      case ExceptionType::NOT_IMPLEMENTED:
        return "Not Implemented";
      case ExceptionType::CATALOG:
        return "Catalog";
      case ExceptionType::MESSENGER:
        return "Messenger";
      case ExceptionType::SETTINGS:
        return "Settings";
      case ExceptionType::BINDER:
        return "Binder";
      case ExceptionType::EXECUTION:
        return "Execution";
      default:
        return "Unknown exception type";
    }
  }

  /**
   * @return the file that threw the exception
   */
  const char *GetFile() const { return file_; }

  /**
   * @return the line number that threw the exception
   */
  int GetLine() const { return line_; }

 protected:
  /**
   * The type of exception.
   */
  const ExceptionType type_;
  /**
   * The name of the file in which the exception was raised.
   */
  const char *file_;
  /**
   * The line number at which the exception was raised.
   */
  const int line_;
};

// -----------------------
// Derived exception types
// -----------------------

#define DEFINE_EXCEPTION(e_name, e_type)                                                                       \
  class e_name : public Exception {                                                                            \
    e_name() = delete;                                                                                         \
                                                                                                               \
   public:                                                                                                     \
    e_name(const char *msg, const char *file, int line) : Exception(e_type, msg, file, line) {}                \
    e_name(const std::string &msg, const char *file, int line) : Exception(e_type, msg.c_str(), file, line) {} \
  }

#define DEFINE_EXCEPTION_WITH_ERRCODE(e_name, e_type)                                  \
  class e_name : public Exception {                                                    \
    e_name() = delete;                                                                 \
                                                                                       \
   public:                                                                             \
    e_name(const char *msg, const char *file, int line, common::ErrorCode code)        \
        : Exception(e_type, msg, file, line), code_(code) {}                           \
    e_name(const std::string &msg, const char *file, int line, common::ErrorCode code) \
        : Exception(e_type, msg.c_str(), file, line), code_(code) {}                   \
                                                                                       \
    /** The SQL error code. */                                                         \
    common::ErrorCode code_;                                                           \
  }

DEFINE_EXCEPTION(NotImplementedException, ExceptionType::NOT_IMPLEMENTED);
DEFINE_EXCEPTION(CatalogException, ExceptionType::CATALOG);
DEFINE_EXCEPTION(ExecutionException, ExceptionType::EXECUTION);



}  // namespace noisepage
