#include <iostream>
#include <thread>
#include <unordered_set>

#include "external/cmd_line_parser/include/parser.hpp"
#include "include/pthash.hpp"
#include "src/util.hpp"

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
    std::string output_filename;
};

enum phf_type { single, partitioned, dense_partitioned };

template <typename Function, typename Builder, typename Iterator>
void build_benchmark(Builder& builder, build_timings const& timings,
                     build_parameters<Iterator> const& params, build_configuration const& config) {
    Function f;
    double encoding_microseconds = f.build(builder, config);

    // timings breakdown
    uint64_t total_microseconds = timings.partitioning_microseconds +
                                  timings.mapping_ordering_microseconds +
                                  timings.searching_microseconds + encoding_microseconds;
    if (config.verbose) {
        std::cout << "partitioning: " << timings.partitioning_microseconds / 1000000.0 << " [sec]"
                  << std::endl;
        std::cout << "mapping+ordering: " << timings.mapping_ordering_microseconds / 1000000.0
                  << " [sec]" << std::endl;
        std::cout << "searching: " << timings.searching_microseconds / 1000000.0 << " [sec]"
                  << std::endl;
        std::cout << "encoding: " << encoding_microseconds / 1000000.0 << " [sec]" << std::endl;
        std::cout << "total: " << total_microseconds / 1000000.0 << " [sec]" << std::endl;
    }

    // space breakdown
    double pt_bits_per_key = static_cast<double>(f.num_bits_for_pilots()) / f.num_keys();
    double mapper_bits_per_key = static_cast<double>(f.num_bits_for_mapper()) / f.num_keys();
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    if (config.verbose) {
        std::cout << "pilots: " << pt_bits_per_key << " [bits/key]" << std::endl;
        std::cout << "mapper: " << mapper_bits_per_key << " [bits/key]" << std::endl;
        std::cout << "total: " << bits_per_key << " [bits/key]" << std::endl;
    }

    // correctness check
    if (params.check) {
        if (config.verbose) essentials::logger("checking data structure for correctness...");
        if (check(params.keys, f) and config.verbose) {
            std::cout << "EVERYTHING OK!" << std::endl;
        }
    }

    double nanosec_per_key = 0;
    if (params.num_queries != 0) {
        if (config.verbose) essentials::logger("measuring lookup time...");
        if (params.external_memory) {
            std::vector<typename Iterator::value_type> queries;
            uint64_t remaining = params.num_queries, batch_size = 100 * 1000000;
            Iterator query = params.keys;
            while (remaining > 0) {
                auto cur_batch_size = std::min(remaining, batch_size);
                queries.reserve(cur_batch_size);
                for (uint64_t i = 0; i != cur_batch_size; ++i, ++query) queries.push_back(*query);
                nanosec_per_key += perf(queries.begin(), cur_batch_size, f) * cur_batch_size;
                remaining -= cur_batch_size;
                queries.clear();
            }
            nanosec_per_key /= params.num_keys;
        } else {
            nanosec_per_key =
                perf(params.keys, std::min<uint64_t>(params.num_queries, f.num_keys()), f);
        }
        if (config.verbose) std::cout << nanosec_per_key << " [nanosec/key]" << std::endl;
    }

    essentials::json_lines result;

    result.add("n", params.num_keys);
    result.add("lambda", config.lambda);
    result.add("alpha", config.alpha);
    result.add("minimal", config.minimal ? "true" : "false");
    result.add("encoder_type", Function::encoder_type::name().c_str());
    result.add("bucketer_type", params.bucketer_type.c_str());
    result.add("secondary_sort", config.secondary_sort);
    result.add("avg_partition_size", config.avg_partition_size);
    result.add("num_partitions",
               compute_num_partitions(params.num_keys, config.avg_partition_size));

    if (config.seed != constants::invalid_seed) result.add("seed", config.seed);

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

template <phf_type t, typename Builder, pthash_search_type search_type, typename Iterator>
void choose_encoder(build_parameters<Iterator> const& params, build_configuration const& config)  //
{
    Builder builder;
    build_timings timings = builder.build_from_keys(params.keys, params.num_keys, config);

    bool encode_all = (params.encoder_type == "all");

    if constexpr (t == phf_type::single)  //
    {
        if (encode_all or params.encoder_type == "D-D") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           dictionary_dictionary,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R-R") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           rice_rice,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           partitioned_compact,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#ifdef PTHASH_ENABLE_ALL_ENCODERS
        if (encode_all or params.encoder_type == "D") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           dictionary,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           rice,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           compact,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "EF") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           elias_fano,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C-C") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           compact_compact,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D-EF") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           dictionary_elias_fano,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "SDC") {
            using function_type =
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           sdc,  //
                           true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#endif
    }                                               //
    else if constexpr (t == phf_type::partitioned)  //
    {
        if (encode_all or params.encoder_type == "D-D") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary_dictionary,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R-R") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                rice_rice,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                partitioned_compact,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#ifdef PTHASH_ENABLE_ALL_ENCODERS
        if (encode_all or params.encoder_type == "D") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "R") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                rice,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                compact,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "EF") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                elias_fano,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "C-C") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                compact_compact,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "D-EF") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary_elias_fano,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "SDC") {
            using function_type =
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                sdc,  //
                                true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#endif
    }                                                     //
    else if constexpr (t == phf_type::dense_partitioned)  //
    {
        if (encode_all or params.encoder_type == "inter-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "inter-C") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_C,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "inter-C-inter-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_C_inter_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#ifdef PTHASH_ENABLE_ALL_ENCODERS
        if (encode_all or params.encoder_type == "mono-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-C") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_C,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-D") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_D,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "inter-D") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_D,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-EF") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_EF,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "inter-EF") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_EF,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-C-mono-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_C_mono_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-D-mono-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        mono_D_mono_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "inter-D-inter-R") {
            using function_type = dense_partitioned_phf<typename Builder::hasher_type,
                                                        typename Builder::bucketer_type,
                                                        inter_D_inter_R,  //
                                                        true, search_type>;
            build_benchmark<function_type>(builder, timings, params, config);
        }
#endif
    } else {
        std::cerr << "unknown phf type" << std::endl;
    }
}

template <phf_type t, typename Builder, typename Iterator>
void choose_search(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (config.search == pthash_search_type::xor_displacement) {
        choose_encoder<t, Builder, pthash_search_type::xor_displacement>(params, config);
    } else if (config.search == pthash_search_type::add_displacement) {
        choose_encoder<t, Builder, pthash_search_type::add_displacement>(params, config);
    } else {
        assert(false);
    }
}

template <typename Hasher, typename Bucketer, typename Iterator>
void choose_builder(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (config.avg_partition_size != 0) {
        if (config.dense_partitioning) {
            if (params.external_memory) {
                assert(false);  // not implemented
            } else {
                choose_search<phf_type::dense_partitioned,  //
                              internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                         config);
            }
        } else {
            if (params.external_memory) {
                choose_search<phf_type::partitioned,  //
                              external_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                         config);
            } else {
                choose_search<phf_type::partitioned,  //
                              internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                         config);
            }
        }
    } else {
        if (params.external_memory) {
            choose_search<phf_type::single,  //
                          external_memory_builder_single_phf<Hasher, Bucketer>>(params, config);
        } else {
            choose_search<phf_type::single,  //
                          internal_memory_builder_single_phf<Hasher, Bucketer>>(params, config);
        }
    }
}

template <typename Hasher, typename Iterator>
void choose_bucketer(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (params.bucketer_type == "uniform") {
        choose_builder<Hasher, uniform_bucketer>(params, config);
    } else if (params.bucketer_type == "skew") {
        choose_builder<Hasher, skew_bucketer>(params, config);
    } else if (params.bucketer_type == "opt") {
        choose_builder<Hasher, table_bucketer<opt_bucketer>>(params, config);
    } else {
        assert(false);
    }
}

template <typename Iterator>
void build(cmd_line_parser::parser const& parser, Iterator keys, uint64_t num_keys) {
    build_parameters<Iterator> params(keys, num_keys);
    params.external_memory = parser.get<bool>("external_memory");
    params.check = parser.get<bool>("check");
    params.num_queries = parser.get<uint64_t>("num_queries");

    params.encoder_type = parser.get<std::string>("encoder_type");
    params.bucketer_type = parser.get<std::string>("bucketer_type");
    {
        std::unordered_set<std::string> encoders({

            "D-D",  // for single and partitioned
            "R-R",
            "PC",

            "inter-R",  // for dense partitioned
            "inter-C",
            "inter-C-inter-R",

#ifdef PTHASH_ENABLE_ALL_ENCODERS

            "D",  // for single and partitioned
            "R",
            "C",
            "EF",
            "C-C",
            "D-EF",
            "SDC",

            "mono-R",  // for dense partitioned
            "mono-C",
            "mono-D",
            "inter-D",
            "mono-EF",
            "inter-EF",
            "mono-C-mono-R",
            "mono-D-mono-R",
            "inter-D-inter-R",
#endif
            "all"  //
        });
        if (encoders.find(params.encoder_type) == encoders.end()) {
            std::cerr << "unknown encoder type" << std::endl;
            return;
        }
        std::unordered_set<std::string> bucketers({"uniform", "skew", "opt"});
        if (bucketers.find(params.bucketer_type) == bucketers.end()) {
            std::cerr << "unknown bucketer type" << std::endl;
            return;
        }
    }

    params.output_filename =
        (!parser.parsed("output_filename")) ? "" : parser.get<std::string>("output_filename");

    build_configuration config;
    config.lambda = parser.get<double>("lambda");
    config.alpha = parser.get<double>("alpha");

    auto search_type = parser.get<std::string>("search_type");
    if (search_type == "xor") {
        config.search = pthash_search_type::xor_displacement;
    } else if (search_type == "add") {
        config.search = pthash_search_type::add_displacement;
    } else {
        std::cerr << "unknown search type" << std::endl;
        return;
    }

    config.minimal = true;
    config.secondary_sort = parser.get<bool>("secondary_sort");
    config.dense_partitioning = parser.get<bool>("dense_partitioning");
    config.verbose = parser.get<bool>("verbose");

    config.avg_partition_size = 0;
    if (parser.parsed("avg_partition_size")) {
        config.avg_partition_size = parser.get<uint64_t>("avg_partition_size");
    }

    if (parser.parsed("num_threads")) {
        config.num_threads = parser.get<uint64_t>("num_threads");
        if (config.num_threads == 0) {
            std::cout << "Warning: specified 0 threads, defaulting to 1" << std::endl;
            config.num_threads = 1;
        }
        uint64_t num_threads = std::thread::hardware_concurrency();
        if (config.num_threads > num_threads) {
            config.num_threads = num_threads;
            std::cout << "Warning: too many threads specified, defaulting to " << config.num_threads
                      << std::endl;
        }
    }

    if (parser.parsed("seed")) config.seed = parser.get<uint64_t>("seed");
    if (parser.parsed("tmp_dir")) config.tmp_dir = parser.get<std::string>("tmp_dir");

    if (parser.parsed("ram")) {
        uint64_t ram = parser.get<double>("ram") * essentials::GB;
        if (ram > constants::available_ram) {
            double available_ram_in_GB =
                static_cast<double>(constants::available_ram) / essentials::GB;
            std::cout << "Warning: too much RAM specified, this machine has " << available_ram_in_GB
                      << " GB of RAM; defaulting to " << available_ram_in_GB * 0.75 << " GB"
                      << std::endl;
            ram = static_cast<double>(constants::available_ram) * 0.75;
        }
        config.ram = ram;
    }

    choose_bucketer<xxhash128>(params, config);
}

int main(int argc, char** argv) {
    cmd_line_parser::parser parser(argc, argv);

    /* Required arguments. */
    parser.add("num_keys", "The size of the input.", "-n", true);
    parser.add("lambda",
               "A constant that trades construction speed for space effectiveness. "
               "A reasonable value lies between 3.0 and 10.0.",
               "-l", true);
    parser.add("alpha", "The table load factor. It must be a quantity > 0 and <= 1.", "-a", true);
    parser.add("search_type", "The pilot search type. Possibile values are: 'xor' and 'add'.", "-r",
               true);

    parser.add("encoder_type",
               "The encoder type. Possibile values are: "
               "'R-R', 'PC', 'inter-R', 'inter-C', 'mono-C-mono-R', "
#ifdef PTHASH_ENABLE_ALL_ENCODERS
               "'D-D', 'EF', "
               "'mono-R', 'mono-C', 'mono-D', 'mono-EF', "
               "'inter-D', 'inter-EF', "
               "'inter-C-inter-R', 'mono-D-mono-R', 'inter-D-inter-R', "
               "'all'.\n\t"
#else
               "(For more encoders, compile again with 'cmake .. -D "
               "PTHASH_ENABLE_ALL_ENCODERS=On').\n\t"
#endif
               "The 'all' type will just benchmark all encoders. (Useful for benchmarking "
               "purposes.)",
               "-e", true);

    parser.add("bucketer_type", "The bucketer type. Possible values are: 'uniform', 'skew', 'opt'.",
               "-b", true);
    parser.add("num_queries", "Number of queries for benchmarking or 0 for no benchmarking.", "-q",
               true, false);

    /* Optional arguments. */
    parser.add("avg_partition_size", "Average partition size.", "-p", false);
    parser.add("seed", "Seed to use for construction.", "-s", false);
    parser.add("num_threads", "Number of threads to use for construction.", "-t", false);
    parser.add("input_filename",
               "A string input file name. If this is not provided, then num_keys 64-bit random "
               "keys will be used as input instead."
               "If, instead, the filename is '-', then input is read from standard input.",
               "-i", false);
    parser.add("output_filename", "Output file name where the function will be serialized.", "-o",
               false);
    parser.add("secondary_sort", "Sort buckets secondarily by increasing expected size.", "--sort",
               false, true);
    parser.add("dense_partitioning", "Activate dense partitioning.", "--dense", false, true);
    parser.add("tmp_dir",
               "Temporary directory used for building in external memory. Default is directory '" +
                   constants::default_tmp_dirname + "'.",
               "-d", false);
    parser.add("ram", "Number of Giga bytes of RAM to use for construction in external memory.",
               "-m", false);
    parser.add("minimal", "Build a minimal PHF.", "--minimal", false, true);
    parser.add("external_memory", "Build the function in external memory.", "--external", false,
               true);
    parser.add("verbose", "Verbose output during construction.", "--verbose", false, true);
    parser.add("check", "Check correctness after construction.", "--check", false, true);

    if (!parser.parse()) return 1;
    if (parser.parsed("input_filename") && parser.get<std::string>("input_filename") == "-" &&
        parser.get<bool>("external_memory")) {
        if (parser.get<bool>("check") || parser.get<bool>("lookup")) {
            std::cerr << "--input_filename - (stdin input) in combination with --external can be "
                         "used only without --check and --lookup"
                      << std::endl;
            return 1;
        }
    }

    auto num_keys = parser.get<uint64_t>("num_keys");
    bool external_memory = parser.get<bool>("external_memory");

    if (parser.parsed("input_filename")) {
        auto input_filename = parser.get<std::string>("input_filename");
        // if (external_memory) {
        //     if (input_filename == "-") {
        //         sequential_lines_iterator keys(std::cin);
        //         build(parser, keys, num_keys);
        //     } else {
        //         mm::file_source<uint8_t> input(input_filename, mm::advice::sequential);
        //         lines_iterator keys(input.data(), input.data() + input.size());
        //         build(parser, keys, num_keys);
        //         input.close();
        //     }
        // } else {
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
        // }
    } else {  // use num_keys random strings
        if (external_memory) {
            std::cout << "Warning: external memory construction with in-memory input" << std::endl;
        }
        // build(parser, generate_benchmark_input(num_keys).begin(), num_keys);
        build(parser, distinct_keys<uint64_t>(num_keys, random_value()).begin(), num_keys);
    }

    return 0;
}