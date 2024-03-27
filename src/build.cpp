#include <iostream>
#include <thread>
#include <unordered_set>

#include "external/cmd_line_parser/include/parser.hpp"
#include "pthash.hpp"
#include "util.hpp"

using namespace pthash;

template <typename Iterator>
struct build_parameters {
    build_parameters(Iterator keys, uint64_t num_keys) : keys(keys), num_keys(num_keys) {}

    Iterator keys;
    uint64_t num_keys;
    bool check;
    uint64_t queries;
    std::string bucketer_type;
    std::string encoder_type;
    double dual_encoder_tradeoff;
    std::string output_filename;
};

enum phf_type { single, partitioned, dense_partitioned };

template <typename Function, typename Builder, typename Iterator>
void build_benchmark(Builder& builder, build_timings const& timings,
                     build_parameters<Iterator> const& params, build_configuration const& config) {
    Function f;
    double encoding_microseconds = f.build(builder, config);

    // timings breakdown
    double total_microseconds = timings.partitioning_microseconds +
                                timings.mapping_ordering_microseconds +
                                timings.searching_microseconds + encoding_microseconds;
    if (config.verbose_output) {
        std::cout << "partitioning: " << timings.partitioning_microseconds / 1000000 << " [sec]"
                  << std::endl;
        std::cout << "mapping+ordering: " << timings.mapping_ordering_microseconds / 1000000
                  << " [sec]" << std::endl;
        std::cout << "searching: " << timings.searching_microseconds / 1000000 << " [sec]"
                  << std::endl;
        std::cout << "encoding: " << encoding_microseconds / 1000000 << " [sec]" << std::endl;
        std::cout << "total: " << total_microseconds / 1000000 << " [sec]" << std::endl;
    }

    // space breakdown
    double pt_bits_per_key = static_cast<double>(f.num_bits_for_pilots()) / f.num_keys();
    double mapper_bits_per_key = static_cast<double>(f.num_bits_for_mapper()) / f.num_keys();
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    if (config.verbose_output) {
        std::cout << "pilots: " << pt_bits_per_key << " [bits/key]" << std::endl;
        std::cout << "mapper: " << mapper_bits_per_key << " [bits/key]" << std::endl;
        std::cout << "total: " << bits_per_key << " [bits/key]" << std::endl;
    }

    // correctness check
    if (params.check) {
        if (config.verbose_output) {
            essentials::logger("checking data structure for correctness...");
        }
        if (check(params.keys, f) and config.verbose_output) {
            std::cout << "EVERYTHING OK!" << std::endl;
        }
    }


    std::string benchResult = "---";
    if (params.queries > 0) {

        if (config.verbose_output) essentials::logger("measuring lookup time...");
        // bench
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint32_t> dis;
        std::vector<std::string> queryInputs;
        queryInputs.reserve(params.queries);
        for (int i = 0; i < params.queries; ++i) {
            uint64_t pos = dis(gen) % params.num_keys;
            queryInputs.push_back(params.keys[pos]);
        }

        essentials::timer<std::chrono::high_resolution_clock, std::chrono::nanoseconds> t;
        t.start();
        for (int i = 0; i < params.queries; ++i) { essentials::do_not_optimize_away(f(queryInputs[i])); }
        t.stop();
        double lookup_time = t.elapsed() / static_cast<double>(params.queries);
        benchResult = std::to_string(lookup_time);
        if (config.verbose_output) std::cout << lookup_time << " [nanosec/key]" << std::endl;
    }

    essentials::json_lines result;

    result.add("n", params.num_keys);
    result.add("lambda", config.lambda);
    result.add("alpha", config.alpha);
    result.add("encoder_type", Function::encoder_type::name().c_str());
    result.add("dual_encoder_tradeoff", params.dual_encoder_tradeoff);
    result.add("bucketer_type", params.bucketer_type.c_str());
    result.add("avg_partition_size", config.avg_partition_size);
    result.add("num_partitions",
               compute_num_partitions(params.num_keys, config.avg_partition_size));

    if (config.seed != constants::invalid_seed) result.add("seed", config.seed);

    result.add("num_threads", config.num_threads);

    result.add("partitioning_seconds", timings.partitioning_microseconds / 1000000);
    result.add("mapping_ordering_seconds", timings.mapping_ordering_microseconds / 1000000);
    result.add("searching_seconds", timings.searching_microseconds / 1000000);
    result.add("encoding_seconds", encoding_microseconds / 1000000);
    result.add("total_seconds", total_microseconds / 1000000);
    result.add("pt_bits_per_key", pt_bits_per_key);
    result.add("mapper_bits_per_key", mapper_bits_per_key);
    result.add("bits_per_key", bits_per_key);
    result.add("secondary_sort", config.secondary_sort);
    result.add("query_time", benchResult.c_str());
    result.print_line();

    if (params.output_filename != "") {
        essentials::logger("saving data structure to disk...");
        essentials::save(f, params.output_filename.c_str());
        essentials::logger("DONE");
    }
}

constexpr uint64_t granularity = 15;
template <typename Builder, typename BaseEncoder1, typename BaseEncoder2,
          pthash_search_type search_type, uint64_t tradeoff = granularity, typename Iterator>
void choose_dual_encoder_tradeoff(build_parameters<Iterator> const& params,
                                  build_configuration const& config, Builder const& builder,
                                  build_timings const& timings) {
    if (tradeoff == uint64_t(std::round(params.dual_encoder_tradeoff * granularity))) {
        build_benchmark<dense_partitioned_phf<
            typename Builder::hasher_type, typename Builder::bucketer_type,
            dual_interleaved<BaseEncoder1, BaseEncoder2, tradeoff, granularity>, true,
            search_type>>(builder, timings, params, config);
    }
    if constexpr (tradeoff > 0) {
        choose_dual_encoder_tradeoff<Builder, BaseEncoder1, BaseEncoder2, search_type, tradeoff - 1,
                                     Iterator>(params, config, builder, timings);
    }
}

template <phf_type t, typename Builder, pthash_search_type search_type, typename Iterator>
void choose_encoder(build_parameters<Iterator> const& params, build_configuration const& config) {
    if (config.verbose_output) essentials::logger("construction starts");

    essentials::timer_type T;  // microseconds
    T.start();
    Builder builder;
    build_timings timings = builder.build_from_keys(params.keys, params.num_keys, config);
    T.stop();
    std::cout << "elapsed time = " << T.elapsed() / 1000000 << " [sec]" << std::endl;

    if (config.verbose_output) essentials::logger("construction ends (no encoding)");

    bool encode_all = (params.encoder_type == "all");

    if constexpr (t == phf_type::single)  //
    {
        if (encode_all or params.encoder_type == "R-R") {
            build_benchmark<
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           rice_rice, true, search_type>>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            build_benchmark<
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           partitioned_compact, true, search_type>>(builder, timings, params,
                                                                    config);
        }
        if (encode_all or params.encoder_type == "D-D") {
            build_benchmark<
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           dictionary_dictionary, true, search_type>>(builder, timings, params,
                                                                      config);
        }
        if (encode_all or params.encoder_type == "EF") {
            build_benchmark<
                single_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                           elias_fano, true, search_type>>(builder, timings, params, config);
        }
    }                                               //
    else if constexpr (t == phf_type::partitioned)  //
    {
        if (encode_all or params.encoder_type == "R-R") {
            build_benchmark<
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                rice_rice, true, search_type>>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "PC") {
            build_benchmark<
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                partitioned_compact, true, search_type>>(builder, timings, params,
                                                                         config);
        }
        if (encode_all or params.encoder_type == "D-D") {
            build_benchmark<
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                dictionary_dictionary, true, search_type>>(builder, timings, params,
                                                                           config);
        }
        if (encode_all or params.encoder_type == "EF") {
            build_benchmark<
                partitioned_phf<typename Builder::hasher_type, typename Builder::bucketer_type,
                                elias_fano, true, search_type>>(builder, timings, params, config);
        }
    }                                                     //
    else if constexpr (t == phf_type::dense_partitioned)  //
    {
        if (encode_all or params.encoder_type == "mono-R") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, mono_R, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "multi-R") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, multi_R, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-C") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, mono_C, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "multi-C") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, multi_C, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-D") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, mono_D, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "multi-D") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, multi_D, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-EF") {
            build_benchmark<
                dense_partitioned_phf<typename Builder::hasher_type,
                                      typename Builder::bucketer_type, mono_EF, true, search_type>>(
                builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "multi-EF") {
            build_benchmark<dense_partitioned_phf<typename Builder::hasher_type,
                                                  typename Builder::bucketer_type, multi_EF, true,
                                                  search_type>>(builder, timings, params, config);
        }
        if (encode_all or params.encoder_type == "mono-C-mono-R") {
            choose_dual_encoder_tradeoff<Builder, mono_C, mono_R, search_type>(params, config,
                                                                               builder, timings);
        }
        if (encode_all or params.encoder_type == "multi-C-multi-R") {
            choose_dual_encoder_tradeoff<Builder, multi_C, multi_R, search_type>(params, config,
                                                                                 builder, timings);
        }
        if (encode_all or params.encoder_type == "mono-D-mono-R") {
            choose_dual_encoder_tradeoff<Builder, mono_D, mono_R, search_type>(params, config,
                                                                               builder, timings);
        }
        if (encode_all or params.encoder_type == "multi-D-multi-R") {
            choose_dual_encoder_tradeoff<Builder, multi_D, multi_R, search_type>(params, config,
                                                                                 builder, timings);
        }

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
            choose_search<phf_type::dense_partitioned,
                          internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                     config);
        } else {
            choose_search<phf_type::partitioned,
                          internal_memory_builder_partitioned_phf<Hasher, Bucketer>>(params,
                                                                                     config);
        }
    } else {
        choose_search<phf_type::single, internal_memory_builder_single_phf<Hasher, Bucketer>>(
            params, config);
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
void choose_hasher(build_parameters<Iterator> const& params, build_configuration const& config) {
    // if (params.num_keys <= (uint64_t(1) << 30)) {
    //     choose_bucketer<murmurhash2_64>(params, config);
    // } else {
    choose_bucketer<xxhash128>(params, config);
    // }
}

template <typename Iterator>
void build(cmd_line_parser::parser const& parser, Iterator keys, uint64_t num_keys) {
    build_parameters<Iterator> params(keys, num_keys);
    params.check = parser.get<bool>("check");
    params.queries = parser.get<uint64_t>("queries");
    params.dual_encoder_tradeoff = parser.get<double>("dual_encoder_tradeoff");

    if (params.dual_encoder_tradeoff < 0.0 || params.dual_encoder_tradeoff > 1.0) {
        std::cerr << "invalid tradeoff" << std::endl;
    }

    params.encoder_type = parser.get<std::string>("encoder_type");
    params.bucketer_type = parser.get<std::string>("bucketer_type");
    {
        std::unordered_set<std::string> encoders({
            //
            "R-R",  // dual Rice
            "PC",   // partitioned compact
            "D-D",  // dual dictionary
            "EF",   // Elias-Fano

            /* only for dense partitioning  */
            "mono-R", "mono-C", "mono-D", "mono-EF",                                 // mono
            "multi-R", "multi-C", "multi-D", "multi-EF",                             // multi
            "mono-C-mono-R", "multi-C-multi-R", "mono-D-mono-R", "multi-D-multi-R",  // dual

            /**/
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

    config.minimal_output = true;
    config.secondary_sort = parser.get<bool>("secondary_sort");
    config.dense_partitioning = parser.get<bool>("dense_partitioning");
    config.verbose_output = parser.get<bool>("verbose_output");

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

    choose_hasher(params, config);
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
               "'R-R', 'PC', 'D-D', 'EF', "
               "'mono-R', 'mono-C', 'mono-D', 'mono-EF', "
               "'multi-R', 'multi-C', 'multi-D', 'multi-EF', "
               "'mono-C-mono-R', 'multi-C-multi-R', 'mono-D-mono-R', 'multi-D-multi-R', "
               "'all'.\n\t"
               "The 'all' type will just benchmark all encoders. (Useful for benchmarking "
               "purposes.)",
               "-e", true);

    parser.add("bucketer_type", "The bucketer type. Possible values are: 'uniform', 'skew', 'opt'.",
               "-b", true);
    parser.add("queries", "Number of queries for benchmarking or 0 for no benchmarking", "-q", true,
               false);

    /* Optional arguments. */
    parser.add("dual_encoder_tradeoff", "Encoder tradeoff when using dual encoding", "-d", false);
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
    parser.add("verbose_output", "Verbose output during construction.", "--verbose", false, true);
    parser.add("check", "Check correctness after construction.", "--check", false, true);

    if (!parser.parse()) return 1;

    auto num_keys = parser.get<uint64_t>("num_keys");
    auto seed = (parser.parsed("seed")) ? parser.get<uint64_t>("seed") : constants::invalid_seed;

    if (parser.parsed("input_filename")) {
        auto input_filename = parser.get<std::string>("input_filename");
        std::vector<std::string> keys;
        if (input_filename == "-") {
            keys = read_string_collection(num_keys, std::cin, parser.get<bool>("verbose_output"));
        } else {
            std::ifstream input(input_filename.c_str());
            if (!input.good()) throw std::runtime_error("error in opening file.");
            keys = read_string_collection(num_keys, input, parser.get<bool>("verbose_output"));
            input.close();
        }
        build(parser, keys.begin(), keys.size());
    } else {  // use num_keys random 64-bit keys
        std::vector<std::string> keys;
        keys.reserve(num_keys);
        for (size_t i = 0; i < num_keys; ++i) { keys.push_back(std::to_string(i)); }
        build(parser, keys.begin(), keys.size());
    }

    return 0;
}