
#pragma once
#include <memory>
#include "Pool.hpp"
template<typename T> using volt_vector = std::vector<T, voltdb::allocator<T>>;
template<typename T> using volt_deque = std::deque<T, std::allocator<T>>;    // NOTE: do not use voltdb::allocator: will crash indeterministically

