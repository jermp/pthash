[![CodeQL](https://github.com/jermp/pthash/actions/workflows/codeql.yml/badge.svg)](https://github.com/jermp/pthash/actions/workflows/codeql.yml)

PTHash / PHOBIC
---------------

PTHash is a C++ library implementing fast and compact minimal perfect hash functions as described in the papers

* [*PTHash: Revisiting FCH Minimal Perfect Hashing*](https://dl.acm.org/doi/10.1145/3404835.3462849)
* [*Parallel and External-Memory Construction of Minimal Perfect Hash Functions with PTHash*](https://ieeexplore.ieee.org/document/10210677)

**PHOBIC** revisits the idea to build smaller functions in less time, for the same query performance as described in the paper

* [*PHOBIC: Perfect Hashing with Optimized Bucket Sizes and Interleaved Coding*](https://arxiv.org/pdf/2404.18497), Stefan Hermann, Hans-Peter Lehmann, Giulio Ermanno Pibiri, Peter Sanders, and Stefan Walzer. To appear in ESA 2024.

Please, cite these papers if you use PTHash or PHOBIC.

#### **NEWS**:

- The [PHOBIC](https://github.com/jermp/pthash/tree/phobic) branch of PTHash introduces some algorithmic novelties to build smaller functions and accelerate construction.

#### Features
- Minimal and Non-Minimal Perfect Hash Functions
- Space/Time Efficiency: fast lookup within compressed space
- Multi-Threaded Construction
- Configurable: can offer different trade-offs (between construction time, lookup time, and space effectiveness)

Introduction
----
Given a set *S* of *n* distinct keys, a function *f* that bijectively maps the keys of *S* into the first *n* natural numbers
is called a *minimal perfect hash function* (MPHF) for *S*.
Algorithms that find such functions when *n* is large and retain constant evaluation time are of practical interest.
For instance, search engines and databases typically use minimal perfect hash functions to quickly assign identifiers to static sets of variable-length keys such as strings.
The challenge is to design an algorithm which is efficient in three different aspects: time to find *f* (construction time), time to evaluate *f* on a key of *S* (lookup time), and space of representation for *f*.

PTHash and PHOBIC are two such algorithms.

The following guide is meant to provide a brief overview of the library
by illustrating its functionalities through some examples.

##### Table of contents
* [Compiling the Code](#compiling-the-code)
* [Quick Start](#quick-start)
* [Build](#build)

Compiling the Code
-----

The code is tested on Linux with `gcc` and on Mac OS with `clang` (both Intel and ARM processors, like Apple M1).
To build the code, [`CMake`](https://cmake.org/) is required.

Clone the repository with

	git clone --recursive https://github.com/jermp/pthash.git
    git checkout phobic

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

### Enable Large Bucket-Id Type
By default, PTHash assumes there are less than $2^{32}$ buckets, hence 32-bit integers are used
for bucket ids. To overcome this, you can either lower the value of `c` or recompile with

    cmake .. -D PTHASH_ENABLE_LARGE_BUCKET_ID_TYPE=On

to use 64-bit integers for bucket ids.

Quick Start
-----

For a quick start, see the source file `src/example.cpp`.
The example shows how to setup a simple build configuration
for PTHash (parameters, base hasher, and encoder).

After compilation, run this example with

	./example

Build
-----

After compilation (see the section [Compiling the Code](#compiling-the-code)),
the driver program called `build` can be used to run some examples.

Run

	./build --help

shows the usage of the driver program.

#### Example

	./build -n 100000 -l 4 -a 0.99 -r xor -e D-D -b skew -q 10000 -s 0 --verbose --check

This example builds a MPHF over 1M random strings, using l = 4.5, alpha = 0.99, seed 0 (`-s`), with xor-type search, compressing the MPHF data structure with the encoder `dictionary_dictionary`, and using the skew bucketer. Also, it will perform 10000 queries (`-q`) and check correctness.
