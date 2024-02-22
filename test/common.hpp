#pragma once

#include <iostream>

#include "pthash.hpp"
#include "utils/util.hpp"
#include "src/util.hpp"

namespace pthash::testing {

template <typename T>
void require_equal(T const& got, T const& required) {
    if (got != required) {
        std::cerr << "got " << got << " but required " << required << std::endl;
        throw std::runtime_error("require_equal");
    }
}

}  // namespace pthash::testing