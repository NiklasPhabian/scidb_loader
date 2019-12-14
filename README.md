# About
scidb_loader is a utility to load VIIRS data to scidb and STARE index

# Required
1. [SciDB-STARE](https://github.com/NiklasPhabian/SciDB-STARE)

SciDB-STARE is the STARE plugin for SciDB.

    iquery -aq "load_library('STARE')"

2. [accelerated_io_tools](accelerated_io_tools)

    iquery -aq "load_library('accelerated_io_tools')"



# Dimensioning
3248 x 3200 = 10 393 600 pixels

At 16 workers: Chunk size 649 600
At 32 workers: Chunk size 324 800
At 64 workers: Chunk size 162 400

at 16 instances: 18.489 S
