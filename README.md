[![Build](https://github.com/jermp/pthash/actions/workflows/build.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/build.yml)
[![CodeQL](https://github.com/jermp/pthash/actions/workflows/codeql.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/codeql.yml)

PTHash
------

PTHash is a C++ library implementing fast and compact minimal perfect hash functions as described in the following research papers:

* [*PTHash: Revisiting FCH Minimal Perfect Hashing*](https://dl.acm.org/doi/10.1145/3404835.3462849),

* [*Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash*](https://ieeexplore.ieee.org/document/10210677),

* [*PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding*](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2024.69).

**Please, cite these papers if you use PTHash.**

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
* [Compiling the code](#compiling-the-code)
* [Quick start](#quick-start)
* [Build examples](#build-examples)
* [Reading keys from standard input ](#reading-keys-from-standard-input)

Integration
-----
Integrating PTHash in your own project is very simple: just get the source code
and include the header `include/pthash.hpp` in your code.
No other configurations are needed.

If you use `git`, the easiest way to add PTHash is via `git add submodule` as follows.

	git submodule add https://github.com/jermp/pthash.git
	
Compiling the Code
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
