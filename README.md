# FlexTree, FlexSpace and FlexDB

This repository contains the reference implementation of FlexTree, FlexSpace and FlexDB presented in the paper
''Building an Efficient Key-Value Store in a Flexible Address Space'' on EuroSys 2022.

[[ACM DL](https://dl.acm.org/doi/10.1145/3492321.3519555)]
[[Paper PDF](https://www.roychan.org/assets/eurosys22-flex/paper.pdf)]
[[Slides](https://www.roychan.org/assets/eurosys22-flex/slides.pdf)]

## Repository Structure

Note: the terms we used in the implementation has some difference from the paper.
Thw following table shows the mapping between the paper terms and API prefixes in the code.

| Paper Term | Code Term |
| --- | --- |
| FlexTree | `flextree` |
| Sorted Extent Array | `brute_force` |
| FlexSpace | `flexfile` |
| FlexSpace Segment | `block` and `BLOCK` in macros |
| FlexDB | `flexdb` |

The content of this repository is organized as follows:
- `c/`: external library dependencies (including the thread-safe skip list used as FlexDB's MemTable)
- `flextree.h`, `flextree.c`: FlexTree implementation and its APIs, including reference code of sorted extent array
- `flexfile.h`, `flexfile.c`: FlexSpace implementation and its APIs
- `flexdb.h`, `flexdb.c`: FlexDB implementation and its APIs
- `generic.h`, `generic.c`: A generic syscall wrapper for different platforms (only Linux currently)
- `wrapper.h`, `wrapper.h`: A library that implements POSIX-file-like interface for FlexSpace
(not coupled with its core functionalities)
- `test_flextree.c`, `test_flexfile.c`, `test_flexdb.c`: Test cases for each component's core functionalities
(correctness only)
- `LICENSE`, `Makefile`: namely

## Implementation Details

You can refer to the header files for more details of the implementation.

### FlexTree and FlexSpace

The FlexTree implementation supports storing the tree in the main memory and using CoW to checkpoint the in-memory
tree into an on-disk file.
The code also contains a naive implementation of a sorted extent array to verify the correctness of FlexTree.
The test program for FlexTree contains ten micro-benchmarks to test its functionalities.

The FlexSpace implementation provides a set of file-like APIs to support
read/(over)write/insert-range/collapse-range/sync operations.
It is backed with the functionalities of log-structured space allocation,
segment-based garbage collection and crash recovery mechanisms.
The test program for FlexSpace tests the basic data I/O operations.
Specifically, the implementation also contains an extra set of APIs that support
setting a tag at a specific logical address.
A tag is a 16-bit value and it shifts with the data.

*Please note that the current implementation of FlexSpace and FlexTree is not thread-safe.*

### FlexDB

The FlexDB implementation provides a fully-functional persistent key-value store.
The code contains the implementation of a volatile sparse KV index, an interval cache,
a skip-list-based MemTable, write-ahead logging and the according crash recovery mechanisms.
FlexDB supports regular KV operations including write/update (`PUT`), point query (`GET`), range query (`SCAN`)
and deletion (`DELETE`).
You can refer to `flexdb.h` to better understand the implementation of FlexDB, as well as the APIs provided.

*The implementation of FlexDB is thread-safe and can be used with multiple concurrent threads.*

## Demo

The test programs for each component is a good starting point to demonstrate the basic usage.
To compile the test program, simply using `make` will compile their executable binaries.
The codebase is verified to compile with the following environment and dependencies:

- Linux kernel 5.10.32 LTS (support io_uring)
- clang 12.0.1
- jemalloc 5.2.1
- liburing 2.0

The basic usage of each testing program is:

- `test_flextree.out`: this program tests the basic functionalities of the FlexTree implementation, and it uses sorted
extent array as reference to verify the correctness. It contains ten test cases and each can be run by using the
command `./test_flextree.out <n>` where `n` is the test ID (0-9). Note that most tests are conducted in DRAM only but
a few tests will use `/tmp` (though it's also usually a memory resident). You can refer to the code to see the content
of each test.
- `test_flexfile.out`: this program is a minimal test to verify the correctness of insertions and deletions in a
FlexSpace. It will create a FlexSpace in `/tmp` and performs a few I/O operations on it, and verify the results with
expected output. You can simply run `./test_flexfile.out` to see if the outputs are correct.
- `test_flexdb.out`: this program tests the basic functionality of FlexDB. It creates a store in `/tmp` and perform a
range of regular KV operations on it. To run the program, simply use `./test_flexdb.out`.

## Citation (BibTeX)

```
@inproceedings{chen2022flexspace,
    author = {Chen, Chen and Zhong, Wenshao and Wu, Xingbo},
    title = {Building an Efficient Key-Value Store in a Flexible Address Space},
    year = {2022},
    isbn = {9781450391627},
    publisher = {Association for Computing Machinery},
    address = {New York, NY, USA},
    url = {https://doi.org/10.1145/3492321.3519555},
    doi = {10.1145/3492321.3519555},
    booktitle = {Proceedings of the Seventeenth European Conference on Computer Systems},
    pages = {51–68},
    numpages = {18},
    keywords = {key-value store, storage, address space},
    location = {Rennes, France},
    series = {EuroSys '22}
}
```
