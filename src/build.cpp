#include <iostream>
#include <thread>
#include <unordered_set>

#include "parser.hpp"
#include "pthash.hpp"
#include "util.hpp"

using namespace pthash;

template <typename Iterator>
struct build_parameters {
    build_parameters(Iterator keys, uint64_t num_keys) : keys(keys), num_keys(num_keys) {}

    Iterator keys;
    uint64_t num_keys;
    uint64_t num_queries;
    bool check;
    bool external_memory;
    std::string bucketer_type;
    std::string encoder_type;
    std::string input_filename;
    std::string output_filename;
};

enum phf_type { single, partitioned, dense_partitioned };

template <typename Function, typename Builder, typename Iterator>
void build_benchmark(Builder& builder, build_timings const& timings,
                     build_parameters<Iterator> const& params, build_configuration const& config) {
    Function f;
    uint64_t encoding_microseconds = f.build(builder, config);

    // timings breakdown
    uint64_t total_microseconds = timings.partitioning_microseconds +
                                  timings.mapping_ordering_microseconds +
                                  timings.searching_microseconds + encoding_microseconds;
    if (config.verbose) {
        std::cout << "=== Construction time breakdown:\n";
        std::cout << "    partitioning: " << timings.partitioning_microseconds / 1000000.0
                  << " [sec]"
                  << " (" << (timings.partitioning_microseconds * 100.0 / total_microseconds)
                  << "%)" << std::endl;
        std::cout << "    mapping+ordering: " << timings.mapping_ordering_microseconds / 1000000.0
                  << " [sec]"
                  << " (" << (timings.mapping_ordering_microseconds * 100.0 / total_microseconds)
                  << "%)" << std::endl;
        std::cout << "    searching: " << timings.searching_microseconds / 1000000.0 << " [sec]"
                  << " (" << (timings.searching_microseconds * 100.0 / total_microseconds) << "%)"
                  << std::endl;
        std::cout << "    encoding: " << encoding_microseconds / 1000000.0 << " [sec]"
                  << " (" << (encoding_microseconds * 100.0 / total_microseconds) << "%)"
                  << std::endl;
        std::cout << "    total: " << total_microseconds / 1000000.0 << " [sec]" << std::endl;
    }

    // space breakdown
    double pt_bits_per_key = static_cast<double>(f.num_bits_for_pilots()) / f.num_keys();
    double mapper_bits_per_key = static_cast<double>(f.num_bits_for_mapper()) / f.num_keys();
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    if (config.verbose) {
        std::cout << "=== Space breakdown:\n";
        std::cout << "    pilots: " << pt_bits_per_key << " [bits/key]"
                  << " (" << (pt_bits_per_key * 100.0) / bits_per_key << "%)" << std::endl;
        std::cout << "    mapper: " << mapper_bits_per_key << " [bits/key]"
                  << " (" << (mapper_bits_per_key * 100.0) / bits_per_key << "%)" << std::endl;
        std::cout << "    total: " << bits_per_key << " [bits/key]" << std::endl;
    }

    // correctness check
    if (params.check) {
        if (config.verbose) essentials::logger("checking data structure for correctness...");
        if (check(params.keys, f) and config.verbose) {
            std::cout << "EVERYTHING OK!" << std::endl;
        }
    }

    // perf lookup queries
    double nanosec_per_key = 0;
    if (params.num_queries != 0 and params.input_filename != "-") {
        if (config.verbose) essentials::logger("measuring lookup time...");
        if (params.external_memory) {
            std::vector<typename Iterator::value_type> queries;
            const uint64_t batch_size = 100 * 1000000;
            uint64_t remaining = std::min<uint64_t>(f.num_keys(), params.num_queries);
            Iterator query = params.keys;
            while (remaining > 0) {
                uint64_t cur_batch_size = std::min<uint64_t>(remaining, batch_size);
                queries.reserve(cur_batch_size);
                for (uint64_t i = 0; i != cur_batch_size; ++i, ++query) queries.push_back(*query);
                nanosec_per_key += perf(queries.begin(), cur_batch_size, f) * cur_batch_size;
                remaining -= cur_batch_size;
                queries.clear();
            }
            nanosec_per_key /= params.num_queries;
        } else {
            nanosec_per_key =
                perf(params.keys, std::min<uint64_t>(params.num_queries, f.num_keys()), f);
        }
        if (config.verbose) std::cout << nanosec_per_key << " [nanosec/key]" << std::endl;
    }

    essentials::json_lines result;

    result.add("n", params.num_keys);
    result.add("lambda", config.lambda);
    if (!config.dense_partitioning) result.add("alpha", config.alpha);
    result.add("minimal", config.minimal ? "true" : "false");
    result.add("encoder_type", Function::encoder_type::name().c_str());
    result.add("bucketer_type", params.bucketer_type.c_str());
    result.add("avg_partition_size", builder.avg_partition_size());
    result.add("num_partitions", builder.num_partitions());
    result.add("dense_partitioning", config.dense_partitioning ? "true" : "false");
    result.add("seed", f.seed());
    result.add("num_threads", config.num_threads);
    result.add("external_memory", params.external_memory ? "true" : "false");
    result.add("partitioning_microseconds", timings.partitioning_microseconds);
    result.add("mapping_ordering_microseconds", timings.mapping_ordering_microseconds);
    result.add("searching_microseconds", timings.searching_microseconds);
    result.add("encoding_microseconds", encoding_microseconds);
    result.add("total_microseconds", total_microseconds);
    result.add("pt_bits_per_key", pt_bits_per_key);
    result.add("mapper_bits_per_key", mapper_bits_per_key);
    result.add("bits_per_key", bits_per_key);
    if (params.num_queries != 0) result.add("nanosec_per_key", nanosec_per_key);

    result.print_line();

    if (params.output_filename != "") {
        essentials::logger("saving data structure to disk...");
        essentials::save(f, params.output_filename.c_str());
        essentials::logger("DONE");
    }
}

template <phf_type t, typename Builder, bool Minimal, typename Iterator>
void choose_encoder(build_parameters<Iterator> const& params, build_configuration const& config)  //
{
    Builder builder;
    build_timings timings = builder.build_from_keys(params.keys, params.num_keys, config);

    const bool encode_all = (params.encoder_type == "all");

    if constexpr (t == phf_type::single)  //
    {
        if (encode_all or params.encoder_type == "C") {
            using function_type = single_phf<typename Builder::hasher_type,
                                             typename Builder::bucketer_type, compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C-C") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           compact_compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D") {
            using function_type = single_phf<typename Builder::hasher_type,
                                             typename Builder::bucketer_type, dictionary, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D-D") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           dictionary_dictionary,  //
                           Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R") {
            using function_type = single_phf<typename Builder::hasher_type,
                                             typename Builder::bucketer_type, rice, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R-R") {
            using function_type = single_phf<typename Builder::hasher_type,
                                             typename Builder::bucketer_type, rice_rice, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "EF") {
            using function_type = single_phf<typename Builder::hasher_type,
                                             typename Builder::bucketer_type, elias_fano, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           partitioned_compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
    }                                               //
    else if constexpr (t == phf_type::partitioned)  //
    {
        if (encode_all or params.encoder_type == "C") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C-C") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                compact_compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D-D") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary_dictionary, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R") {
            using function_type = partitioned_phf<typename Builder::hasher_type,
                                                  typename Builder::bucketer_type, rice, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R-R") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                rice_rice, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "EF") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                elias_fano, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                partitioned_compact, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
    }                                                     //
    else if constexpr (t == phf_type::dense_partitioned)  //
    {
        if (encode_all or params.encoder_type == "C") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, C_mono, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C-int") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, C_int, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, D_mono, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D-int") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, D_int, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, R_mono, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R-int") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, R_int, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "EF") {
            using function_type =
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, EF_mono, Minimal>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
    } else {
        std::cerr << "unknown phf type" << std::endl;
    }
}

template <phf_type t, typename Builder, typename Iterator>
void choose_minimal(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (config.minimal) {
        choose_encoder<t, Builder, true>(params, config);
    } else {
        choose_encoder<t, Builder, false>(params, config);
    }
}

template <typename Hasher, typename Bucketer, typename Iterator>
void choose_builder(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (config.dense_partitioning) {
        if (params.external_memory) {
            std::cerr
                << "Error: External memory construction for dense_partitioned_phf has not been "
                   "implemented yet"
                << std::endl;
            return;
        } else {
            choose_minimal<phf_type::dense_partitioned,  //
                           internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                      config);
        }
    } else {
        if (config.avg_partition_size != 0) {
            if (params.external_memory) {
                choose_minimal<phf_type::partitioned,  //
                               external_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                          config);
            } else {
                choose_minimal<phf_type::partitioned,  //
                               internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                          config);
            }
        } else {
            if (params.external_memory) {
                choose_minimal<phf_type::single,  //
                               external_memory_builder_single_phf<Hasher, Bucketer>>(params,
                                                                                     config);
            } else {
                choose_minimal<phf_type::single,  //
                               internal_memory_builder_single_phf<Hasher, Bucketer>>(params,
                                                                                     config);
            }
        }
    }
}

template <typename Hasher, typename Iterator>
void choose_bucketer(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (params.bucketer_type == "uniform") {
        choose_builder<Hasher, range_bucketer>(params, config);
    } else if (params.bucketer_type == "skew") {
        choose_builder<Hasher, skew_bucketer>(params, config);
    } else if (params.bucketer_type == "opt") {
        choose_builder<Hasher, opt_bucketer>(params, config);
    } else {
        assert(false);
    }
}

template <typename Iterator>
void build(cmd_line_parser::parser const& parser, Iterator keys, uint64_t num_keys) {
    build_parameters<Iterator> params(keys, num_keys);
    params.input_filename = parser.get<std::string>("input_filename");
    params.output_filename =
        (!parser.parsed("output_filename")) ? "" : parser.get<std::string>("output_filename");
    params.external_memory = parser.get<bool>("external_memory");
    params.check = parser.get<bool>("check");
    params.num_queries = parser.get<uint64_t>("num_queries");
    params.encoder_type = parser.get<std::string>("encoder_type");
    params.bucketer_type = parser.get<std::string>("bucketer_type");

    build_configuration config;
    config.dense_partitioning = parser.get<bool>("dense_partitioning");

    {
        std::unordered_set<std::string> encoders_for_single_and_partitioned_phf(
            {"C", "C-C", "D", "D-D", "R", "R-R", "EF", "PC", "all"});
        std::unordered_set<std::string> encoders_for_dense_partitioned_phf(
            {"C", "C-int", "D", "D-int", "R", "R-int", "EF", "all"});

        if (config.dense_partitioning) {
            if (encoders_for_dense_partitioned_phf.find(params.encoder_type) ==
                encoders_for_dense_partitioned_phf.end()) {
                std::cerr << "unknown encoder type for dense_partitioned_phf" << std::endl;
                return;
            }
        } else {
            if (encoders_for_single_and_partitioned_phf.find(params.encoder_type) ==
                encoders_for_single_and_partitioned_phf.end()) {
                std::cerr << "unknown encoder type for single_ and partitioned_ phf" << std::endl;
                return;
            }
        }

        std::unordered_set<std::string> bucketers({"uniform", "skew", "opt"});
        if (bucketers.find(params.bucketer_type) == bucketers.end()) {
            std::cerr << "unknown bucketer type" << std::endl;
            return;
        }
    }

    config.lambda = parser.get<double>("lambda");
    config.minimal = parser.get<bool>("minimal");
    config.verbose = parser.get<bool>("verbose");

    if (parser.parsed("alpha")) config.alpha = parser.get<double>("alpha");

    config.avg_partition_size = 0;
    if (parser.parsed("avg_partition_size")) {
        config.avg_partition_size = parser.get<uint64_t>("avg_partition_size");
    }

    if (parser.parsed("num_threads")) {
        config.num_threads = parser.get<uint64_t>("num_threads");
        if (config.num_threads == 0) {
            if (config.verbose) {
                std::cout << "Warning: specified 0 threads, defaulting to 1" << std::endl;
            }
            config.num_threads = 1;
        }
        uint64_t num_threads = std::thread::hardware_concurrency();
        if (config.num_threads > num_threads) {
            config.num_threads = num_threads;
            if (config.verbose) {
                std::cout << "Warning: too many threads specified, defaulting to "
                          << config.num_threads << std::endl;
            }
        }
    }

    if (parser.parsed("seed")) config.seed = parser.get<uint64_t>("seed");
    if (parser.parsed("tmp_dir")) config.tmp_dir = parser.get<std::string>("tmp_dir");

    if (parser.parsed("ram")) {
        uint64_t ram = parser.get<double>("ram") * essentials::GB;
        if (ram > constants::available_ram) {
            double available_ram_in_GB =
                static_cast<double>(constants::available_ram) / essentials::GB;
            if (config.verbose) {
                std::cout << "Warning: too much RAM specified, this machine has "
                          << available_ram_in_GB << " GB of RAM; defaulting to "
                          << available_ram_in_GB * 0.75 << " GB" << std::endl;
            }
            ram = static_cast<double>(constants::available_ram) * 0.75;
        }
        config.ram = ram;
    }

    choose_bucketer<xxhash_128>(params, config);
}

int main(int argc, char** argv) {
    cmd_line_parser::parser parser(argc, argv);

    /* Required arguments. */
    constexpr bool REQUIRED = true;
    parser.add("num_keys", "The size of the input.", "-n", REQUIRED);
    parser.add("lambda",
               "A constant that trades construction speed for space effectiveness. "
               "A reasonable value lies between 3.0 and 10.0.",
               "-l", REQUIRED);
    parser.add("encoder_type",
               "The encoder type. Possibile values are: "
               "'C', 'C-C', 'D', 'D-D', 'R', 'R-R', 'EF', 'PC' for single and partitioned PHFs; "
               "'C', 'C-int', 'D', 'D-int', 'R', 'R-int', 'EF' for dense partitioned PHFs.\n\t"
               "Specifying 'all' as type will just benchmark all encoders. (Useful for "
               "benchmarking purposes.)",
               "-e", REQUIRED);
    parser.add("bucketer_type", "The bucketer type. Possible values are: 'uniform', 'skew', 'opt'.",
               "-b", REQUIRED);
    parser.add("num_queries", "Number of queries for benchmarking or 0 for no benchmarking.", "-q",
               REQUIRED);

    /* Optional arguments. */
    constexpr bool OPTIONAL = !REQUIRED;
    parser.add("alpha",
               "The table load factor. It must be a quantity > 0 and <= 1 (Defaults is " +
                   std::to_string(constants::default_alpha) + ").",
               "-a", OPTIONAL);
    parser.add("avg_partition_size", "Average partition size for HEM.", "-p", OPTIONAL);
    parser.add("seed", "Seed to use for construction.", "-s", OPTIONAL);
    parser.add("num_threads", "Number of threads to use for construction.", "-t", OPTIONAL);
    parser.add("input_filename",
               "A string input file name. If this is not provided, then [num_keys] 64-bit random "
               "keys will be used as input. "
               "If, instead, the filename is '-', then input is read from standard input.",
               "-i", OPTIONAL);
    parser.add("output_filename", "Output file name where the function will be serialized.", "-o",
               OPTIONAL);
    parser.add("tmp_dir",
               "Temporary directory used for building in external memory. Default is directory '" +
                   constants::default_tmp_dirname + "'.",
               "-d", OPTIONAL);
    parser.add("ram", "Number of Giga bytes of RAM to use for construction in external memory.",
               "-m", OPTIONAL);

    constexpr bool BOOLEAN = true;
    parser.add("minimal", "Build a minimal PHF (MPHF).", "--minimal", OPTIONAL, BOOLEAN);
    parser.add("dense_partitioning", "Activate dense partitioning.", "--dense", OPTIONAL, BOOLEAN);
    parser.add("external_memory", "Build the function in external memory.", "--external", OPTIONAL,
               BOOLEAN);
    parser.add("verbose", "Verbose output during construction.", "--verbose", OPTIONAL, BOOLEAN);
    parser.add("check", "Check correctness after construction.", "--check", OPTIONAL, BOOLEAN);
    parser.add("input-cache",
               "Write the generated input keys to a binary file for later use. (Useful for "
               "benchmarking purposes.)",
               "--cache-input", OPTIONAL, BOOLEAN);

    if (!parser.parse()) return 1;

    if (parser.parsed("input_filename") and                                 //
        parser.get<std::string>("input_filename") == "-" and                //
        parser.get<bool>("external_memory") and parser.get<bool>("check"))  //
    {
        std::cerr << "--input_filename '-' (stdin input) in combination with --external can be "
                     "used only without --check (lookup time cannot be measured either since "
                     "input is only read once)"
                  << std::endl;
        return 1;
    }

    auto num_keys = parser.get<uint64_t>("num_keys");
    bool external_memory = parser.get<bool>("external_memory");

    if (parser.parsed("input_filename")) {
        auto input_filename = parser.get<std::string>("input_filename");
        if (external_memory) {
            if (input_filename == "-") {
                sequential_lines_iterator keys(std::cin);
                build(parser, keys, num_keys);
            } else {
                mm::file_source<uint8_t> input(input_filename, mm::advice::sequential);
                lines_iterator keys(input.data(), input.data() + input.size());
                build(parser, keys, num_keys);
                input.close();
            }
        } else {
            std::vector<std::string> keys;
            if (input_filename == "-") {
                keys = read_string_collection(num_keys, std::cin, parser.get<bool>("verbose"));
            } else {
                std::ifstream input(input_filename.c_str());
                if (!input.good()) throw std::runtime_error("error in opening file.");
                keys = read_string_collection(num_keys, input, parser.get<bool>("verbose"));
                input.close();
            }
            build(parser, keys.begin(), keys.size());
        }
    } else {
        if (external_memory) {
            std::cout << "Warning: external memory construction with in-memory input" << std::endl;
        }

        const uint64_t random_input_seed =
            mix(parser.parsed("seed") ? parser.get<uint64_t>("seed") : 82935257);

        const std::string input_cache_filename =
            "pthash.input-cache.n=" + std::to_string(num_keys) + ".bin";
        std::vector<uint64_t> keys;

        if (parser.get<bool>("input-cache"))  //
        {
            auto generate_and_cache = [&]() {
                keys = distinct_uints<uint64_t>(num_keys, random_input_seed);
                std::ofstream out(input_cache_filename, std::ofstream::binary);
                out.write(reinterpret_cast<char const*>(&random_input_seed), sizeof(uint64_t));
                out.write(reinterpret_cast<char const*>(keys.data()), num_keys * sizeof(uint64_t));
                out.close();
                std::cout << "===> written input keys to '" << input_cache_filename << "'"
                          << std::endl;
            };

            std::ifstream input(input_cache_filename, std::ifstream::binary);
            if (input.good()) {
                uint64_t seed = 0;
                input.read(reinterpret_cast<char*>(&seed), sizeof(uint64_t));
                if (seed == random_input_seed) {
                    keys.resize(num_keys);
                    input.read(reinterpret_cast<char*>(keys.data()), num_keys * sizeof(uint64_t));
                    std::cout << "===> read input keys from '" << input_cache_filename << "'"
                              << std::endl;
                } else {
                    generate_and_cache();
                }
                input.close();
            } else {
                generate_and_cache();
            }
        } else {
            keys = distinct_uints<uint64_t>(num_keys, random_input_seed);
        }

        // build(parser, distinct_strings(num_keys, random_input_seed).begin(), num_keys);
        build(parser, keys.begin(), num_keys);
    }

    return 0;
}