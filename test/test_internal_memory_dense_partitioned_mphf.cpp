#include "common.hpp"

using namespace pthash;
using bucketer_type = skew_bucketer;

template <typename Encoder, typename Builder, typename Iterator>
void test_encoder(Builder& builder, build_configuration const& config, Iterator keys,
                  uint64_t num_keys) {
    dense_partitioned_phf<typename Builder::hasher_type, bucketer_type, Encoder, true> f;
    f.build(builder, config);
    testing::require_equal(f.num_keys(), num_keys);
    check(keys, f);
}

template <typename Iterator>
void test_internal_memory_dense_partitioned_mphf(Iterator keys, uint64_t num_keys) {
    std::cout << "testing on " << num_keys << " keys..." << std::endl;

    internal_memory_builder_partitioned_phf<xxhash_64, bucketer_type> builder_64;
    internal_memory_builder_partitioned_phf<xxhash_128, bucketer_type> builder_128;

    build_configuration config;
    config.minimal = true;
    config.verbose = false;
    config.dense_partitioning = true;
    config.seed = random_value();

    std::vector<double> L{4.0, 4.5, 5.0, 5.5, 6.0};
    for (auto lambda : L) {
        config.lambda = lambda;

        std::cout << "testing with (lambda=" << lambda << ")..." << std::endl;

        builder_64.build_from_keys(keys, num_keys, config);
        test_encoder<mono_R>(builder_64, config, keys, num_keys);           // monoR
        test_encoder<inter_R>(builder_64, config, keys, num_keys);          // interR
        test_encoder<mono_C>(builder_64, config, keys, num_keys);           // monoC
        test_encoder<inter_C>(builder_64, config, keys, num_keys);          // interC
        test_encoder<mono_D>(builder_64, config, keys, num_keys);           // monoD
        test_encoder<inter_D>(builder_64, config, keys, num_keys);          // interD
        test_encoder<mono_EF>(builder_64, config, keys, num_keys);          // monoEF
        test_encoder<inter_EF>(builder_64, config, keys, num_keys);         // interEF
        test_encoder<mono_C_mono_R>(builder_64, config, keys, num_keys);    // monoC-monoR
        test_encoder<inter_C_inter_R>(builder_64, config, keys, num_keys);  // interC-interR
        test_encoder<mono_D_mono_R>(builder_64, config, keys, num_keys);    // monoD-monoR
        test_encoder<inter_D_inter_R>(builder_64, config, keys, num_keys);  // interD-interR

        builder_128.build_from_keys(keys, num_keys, config);
        test_encoder<mono_R>(builder_128, config, keys, num_keys);
        test_encoder<inter_R>(builder_128, config, keys, num_keys);
        test_encoder<mono_C>(builder_128, config, keys, num_keys);
        test_encoder<inter_C>(builder_128, config, keys, num_keys);
        test_encoder<mono_D>(builder_128, config, keys, num_keys);
        test_encoder<inter_D>(builder_128, config, keys, num_keys);
        test_encoder<mono_EF>(builder_128, config, keys, num_keys);
        test_encoder<inter_EF>(builder_128, config, keys, num_keys);
        test_encoder<mono_C_mono_R>(builder_128, config, keys, num_keys);
        test_encoder<inter_C_inter_R>(builder_128, config, keys, num_keys);
        test_encoder<mono_D_mono_R>(builder_128, config, keys, num_keys);
        test_encoder<inter_D_inter_R>(builder_128, config, keys, num_keys);
    }
}

int main() {
    static const uint64_t universe = 100000;
    for (int i = 0; i != 5; ++i) {
        uint64_t num_keys = constants::table_size_per_partition + (random_value() % universe);
        std::vector<uint64_t> keys = distinct_uints<uint64_t>(num_keys, random_value());
        assert(keys.size() == num_keys);
        test_internal_memory_dense_partitioned_mphf(keys.begin(), keys.size());
    }
    return 0;
}
