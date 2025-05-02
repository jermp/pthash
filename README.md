[![Build](https://github.com/jermp/pthash/actions/workflows/build.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/build.yml)
[![CodeQL](https://github.com/jermp/pthash/actions/workflows/codeql.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/codeql.yml)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/pthash_on_dark.png">
  <img src="img/pthash.png" width="350" alt="Logo">
</picture>

**PTHash** is a C++ library implementing fast and compact minimal perfect hash functions as described in the following research papers:

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
| R-R | 0.97 | 3.796 | 11.59 | 33.44 | 22.74 | 65.62 | 0.33 | 0.94 | 34.66 | 2.22 | 65 |
| C-C | 0.99 | 3.796 | 11.57 | 29.75 | 27.26 | 70.08 | 0.07 | 0.17 | 38.90 | 3.25 | 41 |
| D-D | 0.88 | 2.416 | 11.57 | 56.75 | 7.87 | 38.61 | 0.94 | 4.63 | 20.38 | 4.05 | 55 |
| EF | 0.99 | 4.429 | 11.56 | 20.36 | 45.10 | 79.41 | 0.13 | 0.22 | 56.79 | 2.19 | 68 |
| D-D | 0.94 | 3.796 | 11.58 | 36.90 | 19.19 | 61.12 | 0.62 | 1.98 | 31.39 | 3.13 | 46 |

### `pthash::single_phf` with 1 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 128.03 | 17.11 | 616.84 | 82.45 | 3.29 | 0.44 | 748.16 | 2.22 | 130 |
| C-C | 0.99 | 3.796 | 127.89 | 14.60 | 747.21 | 85.32 | 0.70 | 0.08 | 875.80 | 3.25 | 57 |
| D-D | 0.88 | 2.416 | 127.47 | 39.30 | 188.10 | 57.99 | 8.80 | 2.71 | 324.36 | 4.17 | 81 |
| EF | 0.99 | 4.429 | 128.07 | 9.56 | 1210.49 | 90.34 | 1.31 | 0.10 | 1339.87 | 2.19 | 149 |
| D-D | 0.94 | 3.796 | 127.98 | 20.50 | 491.01 | 78.65 | 5.33 | 0.85 | 624.32 | 3.31 | 69 |

### `pthash::partitioned_phf` with 8 thread, 100M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 1.55 | 33.05 | 2.21 | 47.03 | 0.07 | 1.53 | 4.70 | 2.22 | 71 |
| C-C | 0.99 | 3.796 | 1.55 | 31.48 | 2.50 | 50.83 | 0.03 | 0.51 | 4.93 | 3.26 | 43 |
| D-D | 0.88 | 2.416 | 1.56 | 43.87 | 1.01 | 28.39 | 0.13 | 3.69 | 3.55 | 4.05 | 59 |
| EF | 0.99 | 4.429 | 1.55 | 24.24 | 3.95 | 61.82 | 0.04 | 0.65 | 6.39 | 2.19 | 72 |
| D-D | 0.94 | 3.796 | 1.56 | 34.81 | 1.95 | 43.60 | 0.09 | 2.07 | 4.47 | 3.05 | 49 |

### `pthash::partitioned_phf` with 8 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| R-R | 0.97 | 3.796 | 12.84 | 34.51 | 18.77 | 50.47 | 0.45 | 1.20 | 37.20 | 2.22 | 159 |
| C-C | 0.99 | 3.796 | 12.77 | 32.29 | 21.52 | 54.41 | 0.12 | 0.31 | 39.55 | 3.25 | 62 |
| D-D | 0.88 | 2.416 | 12.72 | 45.70 | 8.97 | 32.22 | 0.95 | 3.41 | 27.84 | 4.05 | 90 |
| EF | 0.99 | 4.429 | 12.86 | 24.84 | 33.57 | 64.85 | 0.19 | 0.37 | 51.76 | 2.19 | 159 |
| D-D | 0.94 | 3.796 | 12.75 | 36.59 | 16.33 | 46.84 | 0.61 | 1.74 | 34.85 | 3.05 | 77 |

### `pthash::dense_partitioned_phf` with 8 thread, 100M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| inter-R | 0.97 | 3.796 | 0.93 | 28.76 | 1.37 | 42.58 | 0.25 | 7.80 | 3.22 | 2.35 | 84 |
| inter-C | 0.99 | 3.796 | 0.92 | 27.16 | 1.63 | 47.91 | 0.16 | 4.77 | 3.40 | 3.35 | 56 |
| inter-D | 0.88 | 2.416 | 0.94 | 27.26 | 0.84 | 24.29 | 0.97 | 28.26 | 3.45 | 4.19 | 82 |
| inter-EF | 0.99 | 4.429 | 0.92 | 22.22 | 2.23 | 53.72 | 0.30 | 7.32 | 4.15 | 2.24 | 86 |
| inter-D | 0.94 | 3.796 | 0.93 | 27.13 | 1.19 | 34.71 | 0.64 | 18.67 | 3.44 | 2.99 | 66 |

### `pthash::dense_partitioned_phf` with 8 thread, 1000M keys

| Encoder | $\alpha$ | $\lambda$ | Mapping (sec) | Mapping (%) | Searching (sec) | Searching (%) | Encoding (sec) | Encoding (%) | Total (sec) | Space (bits/key) | Lookup (ns/key) |
|:---------:|:-------:|:--------:|:-------------:|:-------------:|:--------------:|:----------------:|:--------------:|:--------------:|:-----------:|:----------:|:--------------:|
| inter-R | 0.97 | 3.796 | 9.21 | 23.74 | 13.51 | 34.81 | 4.67 | 12.05 | 38.80 | 2.34 | 166 |
| inter-C | 0.99 | 3.796 | 9.22 | 22.97 | 16.23 | 40.42 | 3.63 | 9.03 | 40.14 | 3.45 | 92 |
| inter-D | 0.88 | 2.416 | 9.20 | 19.95 | 8.06 | 17.48 | 17.64 | 38.27 | 46.09 | 4.53 | 126 |
| inter-EF | 0.99 | 4.429 | 9.19 | 18.81 | 22.12 | 45.28 | 6.41 | 13.13 | 48.86 | 2.23 | 172 |
| inter-D | 0.94 | 3.796 | 9.21 | 21.50 | 11.69 | 27.31 | 10.83 | 25.29 | 42.81 | 3.17 | 113 |
