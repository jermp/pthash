#include "common.hpp"

using namespace pthash;

template <typename Encoder, typename Builder, typename Iterator>
void test_encoder(Builder& builder, build_configuration const& config, Iterator keys,
                  uint64_t num_keys) {
    partitioned_phf<typename Builder::hasher_type, Encoder, true> f;
    f.build(builder, config);
    testing::require_equal(f.num_keys(), num_keys);
    check(keys, f);
}

template <typename Iterator>
void test_internal_memory_partitioned_mphf(Iterator keys, uint64_t num_keys) {
    std::cout << "testing on " << num_keys << " keys..." << std::endl;

    internal_memory_builder_partitioned_phf<murmurhash2_64> builder_64;
    internal_memory_builder_partitioned_phf<murmurhash2_128> builder_128;

    build_configuration config;
    config.minimal_output = true;  // mphf
    config.verbose_output = false;
    config.seed = random_value();

    std::vector<uint64_t> num_partitions{1, 16, 32, 64};
    std::vector<double> C{4.0, 4.5, 5.0, 5.5, 6.0};
    std::vector<double> A{1.0, 0.99, 0.98, 0.97, 0.96};
    for (auto c : C) {
        config.c = c;
        for (auto alpha : A) {
            config.alpha = alpha;

            for (auto p : num_partitions) {
                config.num_partitions = p;

                std::cout << "testing with (c=" << c << ";alpha=" << alpha
                          << ";num_partitions=" << p << ")..." << std::endl;

                builder_64.build_from_keys(keys, num_keys, config);
                test_encoder<compact>(builder_64, config, keys, num_keys);
                test_encoder<partitioned_compact>(builder_64, config, keys, num_keys);
                test_encoder<compact_compact>(builder_64, config, keys, num_keys);
                test_encoder<dictionary>(builder_64, config, keys, num_keys);
                test_encoder<dictionary_dictionary>(builder_64, config, keys, num_keys);
                test_encoder<elias_fano>(builder_64, config, keys, num_keys);
                test_encoder<dictionary_elias_fano>(builder_64, config, keys, num_keys);
                test_encoder<sdc>(builder_64, config, keys, num_keys);

                builder_128.build_from_keys(keys, num_keys, config);
                test_encoder<compact>(builder_128, config, keys, num_keys);
                test_encoder<partitioned_compact>(builder_128, config, keys, num_keys);
                test_encoder<compact_compact>(builder_128, config, keys, num_keys);
                test_encoder<dictionary>(builder_128, config, keys, num_keys);
                test_encoder<dictionary_dictionary>(builder_128, config, keys, num_keys);
                test_encoder<elias_fano>(builder_128, config, keys, num_keys);
                test_encoder<dictionary_elias_fano>(builder_128, config, keys, num_keys);
                test_encoder<sdc>(builder_128, config, keys, num_keys);
            }
        }
    }
}

int main() {
    static const uint64_t universe = 1000000;
    for (int i = 0; i != 5; ++i) {
        uint64_t num_keys = universe;  // random_value() % universe;
        if (num_keys < 2) num_keys = 2;
        std::vector<uint64_t> keys = distinct_keys<uint64_t>(num_keys, random_value());
        assert(keys.size() == num_keys);
        test_internal_memory_partitioned_mphf(keys.begin(), keys.size());
    }
    return 0;
}
