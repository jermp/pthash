[![Build](https://github.com/jermp/pthash/actions/workflows/build.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/build.yml)
[![CodeQL](https://github.com/jermp/pthash/actions/workflows/codeql.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/codeql.yml)

PTHash / PHOBIC
---------------

PTHash is a C++ library implementing fast and compact minimal perfect hash functions as described in the following research papers:

- [*PTHash: Revisiting FCH Minimal Perfect Hashing*](https://dl.acm.org/doi/10.1145/3404835.3462849) (SIGIR 2021),
- [*Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash*](https://ieeexplore.ieee.org/document/10210677) (TKDE 2023),
- [*PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding*](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2024.69) (ESA 2024).

**Please, cite these papers if you use PTHash or PHOBIC.**

### Development note

The description of PTHash in the SIGIR and TKDE papers uses the `c` parameter
to control the number of buckets used during the search.
You can get a version of the library using the `c` parameter [here](https://github.com/jermp/pthash/releases/tag/v2.0.0) (Release v2).
The current library
uses instead a parameter called "lambda", as described in the ESA paper.

### Features

- Minimal and Non-Minimal Perfect Hash Functions
- Space/Time Efficiency: fast lookup within compressed space
- External-Memory Scaling
- Multi-Threaded Construction
- Configurable: can offer different trade-offs (between construction time, lookup time, and space effectiveness)

Introduction
----
Given a set *S* of *n* distinct keys, a function *f* that bijectively maps the keys of *S* into the first *n* natural numbers
is called a *minimal perfect hash function* (MPHF) for *S*.
Algorithms that find such functions when *n* is large and retain constant evaluation time are of practical interest.
For instance, search engines and databases typically use minimal perfect hash functions to quickly assign identifiers to static sets of variable-length keys such as strings.
The challenge is to design an algorithm which is efficient in three different aspects: time to find *f* (construction time), time to evaluate *f* on a key of *S* (lookup time), and space of representation for *f*.

**PTHash** is one such algorithm.

The following guide is meant to provide a brief overview of the library
by illustrating its functionalities through some examples.

### Table of contents

* [Integration](#integration)
* [Compiling the benchmark and example code](#compiling-the-code)
* [Quick start](#quick-start)
* [Build examples](#build-examples)
* [Reading keys from standard input ](#reading-keys-from-standard-input)
* [An example benchmark](#an-example-benchmark)

Integration
-----
Integrating PTHash in your own project is very simple.
If you use `git`, the easiest way to add PTHash is via `git add submodule` as follows.

	git submodule add https://github.com/jermp/pthash.git
    git submodule update --recursive --init

Then include the following in your `CMakeLists.txt`, which takes care of
setting up the include paths and compiler flags of PTHash and its dependencies:

    add_subdirectory(pthash)
    target_link_libraries(MyTarget INTERFACE PTHASH)

To construct a perfect hash function, include `pthash.hpp` and create an instance of `pthash::single_phf<...>` (PTHash),
`pthash::partitioned_phf<...>` (PTHash-HEM), or `pthash::dense_partitioned_phf<...>` (PHOBIC).
For convenience, we also give `pthash::phobic<...>` which includes the configuration options for
optimized bucket assignment function (OB) and interleaved coding (IC). Refer to `src/example.cpp` for an example.

Compiling the Benchmark and Example Code
-----

The code is tested on Linux with `gcc` and on Mac OS with `clang` (both Intel and ARM processors, like Apple M1).
To build the code, [`CMake`](https://cmake.org/) is required.

Clone the repository with

	git clone --recursive https://github.com/jermp/pthash.git

If you have cloned the repository **without** `--recursive`, be sure you pull the dependencies with the following command before
compiling:

    git submodule update --init --recursive

To compile the code for a release environment (see file `CMakeLists.txt` for the used compilation flags), it is sufficient to do the following:

    mkdir build
    cd build
    cmake ..
    make -j

For a testing environment, use the following instead:

    mkdir debug_build
    cd debug_build
    cmake .. -D CMAKE_BUILD_TYPE=Debug -D PTHASH_USE_SANITIZERS=On
    make

(NOTE: Beware that the software will result in a much slower execution when running in debug mode and using sanitizers. Use this only for debug purposes, not to run performance tests.)

### Enable All Encoders

By default, you can choose between three encoders to compress the PTHash
data structure (see the output of `./build --help` for suggestions).

If you want to test all the encoders we tested in the papers,
compile again with

	cmake .. -D PTHASH_ENABLE_ALL_ENCODERS=On

### Enable Large Bucket-Id Type

By default, PTHash assumes there are less than $2^{32}$ buckets, hence 32-bit integers are used
for bucket ids. To overcome this, you can either lower the value of `c` or recompile with

    cmake .. -D PTHASH_ENABLE_LARGE_BUCKET_ID_TYPE=On

to use 64-bit integers for bucket ids.

Quick Start
-----

For a quick start, see the source file `src/example.cpp`.
The example shows how to setup a simple build configuration
for PTHash (parameters, base hasher, search type, and encoder).

After compilation, run this example with

	./example

Build Examples
-----

After compilation (see the section [Compiling the Code](#compiling-the-code)),
the driver program called `build` can be used to run some examples.

Run

	./build --help

shows the usage of the driver program. In the following, we illustrate some examples using this `./build` program.

### Example 1

The command

	./build -n 10000000 -l 4.3 -a 0.99 -r xor -e D-D -b skew -q 3000000 -s 0 -p 2000000 -t 8 --verbose --minimal --check

builds a MPHF over 10M random 64-bit integers, using

- avg. bucket size of 4.3 (`-l = 4.3`);
- load factor of 0.99 (`-a 0.99`);
- xor-type search (`-r xor`);
- the encoder `D-D` to compress the data structure;
- the `skew` bucketer type (`-b skew`);
- seed 0 (`-s 0`);
- avg. partition size 2,000,000 (`-p 2000000`);
- 8 parallel threads (`-t 8`).

Also, it performs 3M queries (`-q 3000000`) to benchmark the speed of lookup queries and check the correctness of the function.

### Example 2

For the following example,
we are going to use the strings from the UK-2005 URLs collection,
which can be downloaded by clicking
[here](http://data.law.di.unimi.it/webdata/uk-2005/uk-2005.urls.gz).
(This is also one of the datasets used in the paper.)

The file is ~300 MB compressed using gzip (2.86 GB uncompressed).

After download, place the dataset in the `build` directory and run

	gunzip uk-2005.urls.gz

to uncompress it.
The file contains one string per line, for a total of 39,459,925 strings.

#### NOTE: Input files are read line by line (i.e., individual strings are assumed to be separated by the character `\n`). Be sure there are no blank lines.

The command

	./build -n 39459925 -i ~/Downloads/uk-2005.urls -l 3.5 -a 0.94 -e inter-R -r add -b skew -s 0 -q 3000000 -p 2500 -t 8 --dense --minimal --verbose --check

builds a MPHF using the strings of the file as input keys, where

- the function is of type *dense partitioned"* because option `--dense` is specified, with an avg. partition size of 2500 (`-p 2500`);
- the avg. number of buckets is set to 3.5 (`-l 3.5`);
- the load factor is set to 0.94 (`-a 0.94`);
- the data structure is compressed using interleaved Rice codes (`-e inter-R`);
- the search algorithm is additive displacement (`-r add`);
- the type of bucketer used is `skew`;
- the seed used for the construction is 0 (`-s 0`);
- the number of threads used for the construction are 8 (`-t 8`);
- 3M lookups are performed to benchmark query time (`-q 3000000`).

### Example 3

The command

	./build -n 39459925 -i ~/Downloads/uk-2005.urls -l 3.5 -a 0.94 -e PC -r add -b skew -s 0 -q 3000000 -p 2500000 -t 8 -m 1 --minimal --verbose --check --external

builds a MPHF using most of the parameters used in Example 2 but the function is built in external memory (option `--external`) using 1 GB of RAM (option `-m 1`), and with avg. partition size of 2.5M and compressing the data structure with a partitioned compact encoding (`-e PC`).

Reading Keys from Standard Input
-----

You can make the `build` tool read the keys from stardard input using bash pipelining (`|`)
in combination with option `-i -`. This is very useful when building keys from compressed files.

Some examples below.

	for i in $(seq 1 1000000) ; do echo $i ; done > foo.txt
	cat foo.txt | ./build --minimal -l 3.4 -a 0.94 -e R-R -r add -b skew -n 1000000 -q 0 -m 1 -i - -o foo.mph --verbose --external

	gzip foo.txt
	zcat foo.txt.gz | ./build --minimal -l 3.4 -a 0.94 -e R-R -r add -b skew -n 1000000 -q 0 -m 1 -i - -o foo.mph --verbose --external

	gunzip foo.txt.gz
	zstd foo.txt
	zstdcat foo.txt.zst | ./build --minimal -l 3.4 -a 0.94 -e R-R -r add -b skew -n 1000000 -q 0 -m 1 -i - -o foo.mph --verbose --external

**Note**: you may need to write `zcat < foo.txt.gz | (...)` on Mac OSX.

One caveat of this approach is that it is **not** possible to use `--check` nor benchmark query time because these two options need to re-iterate over the keys from the stream.

An example benchmark
-----

The script `script/run_benchmark.sh` runs some trade-off configurations (encoder, $\alpha$, $\lambda$) that have been tested in the papers, on 100M and 1000M keys.

Be sure you run the benchmark after compiling with

	cmake .. -D PTHASH_ENABLE_ALL_ENCODERS=On

From within the directory where the code has been compiled, just run

	bash ../script/run_benchmark.sh 100000000 2> results.json
	python3 ../script/make_markdown_table.py results.json results.md

to collect the results on an input of 100M keys. (All constructions run in internal memory).

Below, the result of the benchmark (see also the files `script/results.100M.json` and `script/results.1000M.json`) on a machine equipped with
an Intel Xeon W-2245 CPU @ 3.90GHz and running Ubuntu 18.04.6 LTS (GNU/Linux 5.4.0-150-generic x86_64).
The code has been compiled with gcc 9.4.0, with flags `-O3` and `-march=native` in all cases.

### `pthash::single_phf` with 1 thread, 100M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 11.55 | 32.67 | 23.48 | 66.41 | 0.33 | 0.93 | 35.35 | 2.29 | 64 |
| C-C | 0.99 | 3.796 | 11.58 | 29.26 | 27.93 | 70.57 | 0.07 | 0.17 | 39.58 | 3.25 | 39 |
| D-D | 0.88 | 2.416 | 11.59 | 55.76 | 8.24 | 39.66 | 0.95 | 4.58 | 20.78 | 4.05 | 56 |
| EF | 0.99 | 4.429 | 11.55 | 19.63 | 47.17 | 80.16 | 0.13 | 0.21 | 58.84 | 2.26 | 69 |
| D-D | 0.94 | 3.796 | 11.58 | 36.30 | 19.70 | 61.75 | 0.62 | 1.96 | 31.90 | 3.13 | 47 |

### `pthash::single_phf` with 1 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 127.39 | 17.33 | 604.22 | 82.22 | 3.30 | 0.45 | 734.91 | 2.29 | 126 |
| C-C | 0.99 | 3.796 | 127.38 | 14.56 | 746.65 | 85.36 | 0.70 | 0.08 | 874.72 | 3.25 | 57 |
| D-D | 0.88 | 2.416 | 126.88 | 39.55 | 185.17 | 57.71 | 8.79 | 2.74 | 320.84 | 4.17 | 80 |
| EF | 0.99 | 4.429 | 128.03 | 9.57 | 1208.24 | 90.33 | 1.29 | 0.10 | 1337.56 | 2.26 | 153 |
| D-D | 0.94 | 3.796 | 128.71 | 20.65 | 489.23 | 78.48 | 5.41 | 0.87 | 623.35 | 3.31 | 70 |

### `pthash::partitioned_phf` with 8 thread, 100M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 1.58 | 33.34 | 2.21 | 46.67 | 0.07 | 1.53 | 4.74 | 2.29 | 70 |
| C-C | 0.99 | 3.796 | 1.58 | 31.54 | 2.54 | 50.63 | 0.02 | 0.50 | 5.01 | 3.26 | 43 |
| D-D | 0.88 | 2.416 | 1.58 | 43.74 | 1.03 | 28.47 | 0.14 | 3.80 | 3.62 | 4.05 | 60 |
| EF | 0.99 | 4.429 | 1.58 | 24.49 | 3.93 | 60.83 | 0.04 | 0.60 | 6.46 | 2.26 | 75 |
| D-D | 0.94 | 3.796 | 1.58 | 35.27 | 1.93 | 43.01 | 0.10 | 2.18 | 4.48 | 3.05 | 49 |

### `pthash::partitioned_phf` with 8 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 13.13 | 34.70 | 19.02 | 50.29 | 0.48 | 1.28 | 37.83 | 2.29 | 158 |
| C-C | 0.99 | 3.796 | 13.01 | 32.61 | 21.59 | 54.12 | 0.12 | 0.31 | 39.90 | 3.25 | 63 |
| D-D | 0.88 | 2.416 | 12.95 | 45.76 | 9.19 | 32.49 | 0.95 | 3.36 | 28.30 | 4.05 | 91 |
| EF | 0.99 | 4.429 | 13.12 | 25.06 | 33.90 | 64.73 | 0.21 | 0.39 | 52.37 | 2.26 | 180 |
| D-D | 0.94 | 3.796 | 13.01 | 36.72 | 16.66 | 47.00 | 0.62 | 1.75 | 35.44 | 3.05 | 78 |

### `pthash::dense_partitioned_phf` with 8 thread, 100M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| inter-R | 0.97 | 3.796 | 0.93 | 29.00 | 1.37 | 42.65 | 0.25 | 7.73 | 3.21 | 2.43 | 85 |
| inter-C | 0.99 | 3.796 | 0.93 | 27.27 | 1.63 | 48.00 | 0.17 | 5.02 | 3.39 | 3.35 | 56 |
| inter-D | 0.88 | 2.416 | 0.95 | 27.81 | 0.83 | 24.49 | 0.94 | 27.75 | 3.40 | 4.19 | 82 |
| inter-EF | 0.99 | 4.429 | 0.93 | 22.43 | 2.23 | 54.09 | 0.31 | 7.41 | 4.13 | 2.31 | 88 |
| inter-D | 0.94 | 3.796 | 0.93 | 27.19 | 1.19 | 34.74 | 0.64 | 18.80 | 3.43 | 2.99 | 66 |

### `pthash::dense_partitioned_phf` with 8 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| inter-R | 0.97 | 3.796 | 9.32 | 23.90 | 13.73 | 35.21 | 4.65 | 11.93 | 39.01 | 2.42 | 191 |
| inter-C | 0.99 | 3.796 | 9.17 | 23.06 | 16.17 | 40.65 | 3.24 | 8.15 | 39.78 | 3.45 | 94 |
| inter-D | 0.88 | 2.416 | 9.22 | 19.85 | 8.08 | 17.38 | 17.93 | 38.60 | 46.46 | 4.53 | 125 |
| inter-EF | 0.99 | 4.429 | 9.18 | 18.76 | 22.10 | 45.16 | 6.52 | 13.33 | 48.95 | 2.30 | 186 |
| inter-D | 0.94 | 3.796 | 9.22 | 21.41 | 11.73 | 27.24 | 10.98 | 25.51 | 43.06 | 3.17 | 113 |
