#pragma once

#include "../encoders/bit_vector.hpp"

namespace pthash {

void fill_free_slots(bit_vector_builder const& taken, uint64_t num_keys,
                     std::vector<uint64_t>& free_slots) {
    uint64_t table_size = taken.size();
    if (table_size > num_keys) {
        // find holes up to, and including, position num_keys - 1
        free_slots.resize(table_size - num_keys, 0);
        std::vector<uint64_t> slots;
        slots.reserve(table_size - num_keys);
        for (uint64_t i = 0; i != num_keys; ++i) {
            if (taken.get(i) == false) slots.push_back(i);
        }

        // now for each key mapped to the right of position num_keys - 1, assign a new position
        uint64_t count = 0;
        for (uint64_t i = num_keys; i != table_size; ++i) {
            if (taken.get(i) == true) free_slots[i - num_keys] = slots[count++];
        }

        // assign sorted values
        count = 0;
        uint64_t val = 0;
        for (; count != free_slots.size(); ++count) {
            if (free_slots[count] != 0) break;
        }
        for (; count != free_slots.size(); ++count) {
            if (free_slots[count] == 0) {
                free_slots[count] = val;
            } else {
                val = free_slots[count];
            }
        }
    }
}

}  // namespace pthash