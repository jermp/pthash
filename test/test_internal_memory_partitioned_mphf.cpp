#include "common.hpp"

using namespace pthash;
using bucketer_type = skew_bucketer;

template <typename Encoder, typename Builder, typename Iterator>
void test_encoder(Builder& builder, build_configuration const& config, Iterator keys,
                  uint64_t num_keys) {
    partitioned_phf<typename Builder::hasher_type, bucketer_type, Encoder, true,
                    pthash_search_type::xor_displacement>
        f_xor;
    f_xor.build(builder, config);
    testing::require_equal(f_xor.num_keys(), num_keys);
    check(keys, f_xor);
}

template <typename Iterator>
void test_internal_memory_partitioned_mphf(Iterator keys, uint64_t num_keys) {
    std::cout << "testing on " << num_keys << " keys..." << std::endl;

    internal_memory_builder_partitioned_phf<murmurhash2_64, bucketer_type> builder_64;
    internal_memory_builder_partitioned_phf<murmurhash2_128, bucketer_type> builder_128;

    build_configuration config;
    config.search = pthash_search_type::xor_displacement;
    config.minimal_output = true;  // mphf
    config.verbose_output = false;
    config.seed = random_value();

    std::vector<uint64_t> avg_partition_size{1000, 10000, 100000, 1000000};
    std::vector<double> L{4.0, 4.5, 5.0, 5.5, 6.0};
    std::vector<double> A{1.0, 0.99, 0.98, 0.97, 0.96};
    for (auto lambda : L) {
        config.lambda = lambda;
        for (auto alpha : A) {
            config.alpha = alpha;

            for (auto p : avg_partition_size) {
                config.avg_partition_size = p;

                std::cout << "testing with (lambda=" << lambda << "; alpha=" << alpha
                          << "; num_partitions="
                          << compute_num_partitions(num_keys, config.avg_partition_size) << ")..."
                          << std::endl;

                // config.print();

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
    static const uint64_t universe = 100000;
    for (int i = 0; i != 5; ++i) {
        uint64_t num_keys = random_value() % universe;
        if (num_keys < 2) num_keys = 2;
        std::vector<uint64_t> keys = distinct_keys<uint64_t>(num_keys, random_value());
        assert(keys.size() == num_keys);
        test_internal_memory_partitioned_mphf(keys.begin(), keys.size());
    }
    return 0;
}
