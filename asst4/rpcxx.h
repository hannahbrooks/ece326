// -*- c++ -*-
#ifndef RPCXX_SAMPLE_H
#define RPCXX_SAMPLE_H

#include <cstdlib>
#include "rpc.h"

namespace rpc {

// Protocol is used for encode and decode a type to/from the network.
//
// You may use network byte order, but it's optional. We won't test your code
// on two different architectures.

// TASK1: add more specializations to Protocol template class to support more
// types.
template <typename T> struct Protocol;

template <typename T> struct four_byte {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const T &x) {
    if (*out_len < 4) return false;
    uint32_t bx = htonl((uint32_t) x);
    memcpy(out_bytes, &bx, 4);
    *out_len = 4;
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, bool *ok, T &x) {
    if (*in_len < 4) return false;
    uint32_t bx = 0;
    memcpy(&bx, in_bytes, 4);
    x = ntohl(bx);
    *in_len = 4;
    return true;
  }
};

template <> struct Protocol<std::string> {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const std::string &x) {
    int len = x.length();
    uint32_t bit_len = 8 + (~(0x03) & (len - 1)); 
    if (*out_len < bit_len) return false;
    int bx = htobe32(len); 
    memcpy(out_bytes, &bx, 4);
    memcpy(out_bytes + 4, x.data(), len);
    *out_len = bit_len;
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, bool *ok, std::string &x) {
    if (*in_len < 4) return false;
    int bx = 0;
    memcpy(&bx, in_bytes, 4);
    int content = be32toh(bx); 
    uint32_t bit_len = 8 + ((content - 1) & (~0x03)); 
    if (*in_len < bit_len) return false;
    if (content == 0) x = std::string("");
    else {
      x = std::string(content, '\0'); 
      memcpy(&x.front(), in_bytes + 4, content);
    }
    *in_len = bit_len; 
    return true; 
  }
};

template <typename T> struct eight_byte {
  static bool Encode(uint8_t *out_bytes, uint32_t *out_len, const T &x) {
    if (*out_len < 8) return false;
    uint64_t bx = htobe64((uint64_t) x);
    memcpy(out_bytes, &bx, 8);
    *out_len = 8;
    return true;
  }
  static bool Decode(uint8_t *in_bytes, uint32_t *in_len, bool *ok, T &x) {
    if (*in_len < 8) return false;
    uint64_t bx = 0;
    memcpy(&bx, in_bytes, 8);
    x = be64toh(bx);
    *in_len = 8;
    return true;
  }
};

template <> struct Protocol<int> : public four_byte<int>{};
template <> struct Protocol<bool> : public four_byte<bool>{};
template <> struct Protocol<char> : public four_byte<char>{};
template <> struct Protocol<short> : public four_byte<short>{};
template <> struct Protocol<long> : public eight_byte<long>{};
template <> struct Protocol<long long> : public eight_byte<long long>{};
template <> struct Protocol<float> : public eight_byte<float>{};
template <> struct Protocol<double> : public eight_byte<double>{};
template <> struct Protocol<unsigned int> : public four_byte<unsigned int>{};
template <> struct Protocol<unsigned char> : public four_byte<unsigned char>{};
template <> struct Protocol<unsigned short> : public four_byte<unsigned short>{};
template <> struct Protocol<unsigned long> : public eight_byte<unsigned long>{};
template <> struct Protocol<unsigned long long> : public eight_byte<unsigned long long>{};
// template <> struct Protocol<unsigned float> : public eight_byte<unsigned float>{};
// template <> struct Protocol<unsigned double> : public eight_byte<unsigned double>{};

// TASK2: Client-side
class IntParam : public BaseParams {
  int p;
 public:
  IntParam(int p) : p(p) {}

  bool Encode(uint8_t *out_bytes, uint32_t *out_len) const override {
    return Protocol<int>::Encode(out_bytes, out_len, p);
  }
};

// TASK2: Server-side
template <typename Svc>
class IntIntProcedure : public BaseProcedure {
  bool DecodeAndExecute(uint8_t *in_bytes, uint32_t *in_len,
                        uint8_t *out_bytes, uint32_t *out_len,
                        bool *ok) override final {
    int x;
    // This function is similar to Decode. We need to return false if buffer
    // isn't large enough, or fatal error happens during parsing.
    if (!Protocol<int>::Decode(in_bytes, in_len, ok, x) || !*ok) {
      return false;
    }
    // Now we cast the function pointer func_ptr to its original type.
    //
    // This incomplete solution only works for this type of member functions.
    using FunctionPointerType = int (Svc::*)(int);
    auto p = func_ptr.To<FunctionPointerType>();
    int result = (((Svc *) instance)->*p)(x);
    if (!Protocol<int>::Encode(out_bytes, out_len, result)) {
      // out_len should always be large enough so this branch shouldn't be
      // taken. However just in case, we return an fatal error by setting *ok
      // to false.
      *ok = false;
      return false;
    }
    return true;
  }
};

// TASK2: Client-side
class IntResult : public BaseResult {
  int r;
 public:
  bool HandleResponse(uint8_t *in_bytes, uint32_t *in_len, bool *ok) override final {
    return Protocol<int>::Decode(in_bytes, in_len, ok, r);
  }
  int &data() { return r; }
};

// TASK2: Client-side
class Client : public BaseClient {
 public:
  template <typename Svc>
  IntResult *Call(Svc *svc, int (Svc::*func)(int), int x) {
    // Lookup instance and function IDs.
    int instance_id = svc->instance_id();
    int func_id = svc->LookupExportFunction(MemberFunctionPtr::From(func));

    // This incomplete solution only works for this type of member functions.
    // So the result must be an integer.
    auto result = new IntResult();

    // We also send the paramters of the functions. For this incomplete
    // solution, it must be one integer.
    if (!Send(instance_id, func_id, new IntParam(x), result)) {
      // Fail to send, then delete the result and return nullptr.
      delete result;
      return nullptr;
    }
    return result;
  }
};

// TASK2: Server-side
template <typename Svc>
class Service : public BaseService {
 protected:
  void Export(int (Svc::*func)(int)) {
    ExportRaw(MemberFunctionPtr::From(func), new IntIntProcedure<Svc>());
  }
};

}

#endif /* RPCXX_SAMPLE_H */
