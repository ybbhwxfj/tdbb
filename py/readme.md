# Generate configure files

File az1.txt, az2.txt, az2.txt would be read by configure.py to generate configure files.
The priority to become new leader, az3 > az2 > az3.

Ex1: Generate DB Shared , tight bind

    python3 configure.py -s 1 -tb -p ./az_sdb_tight

Ex2: Generate DB Shared CCB and RLB, Scale DSB by 5 shards

    python3 configure.py -s 1 -s -p ./az_scrdb

# Compute line of code

Python mloc.py would compute blocks' lines of code

# Run benchmark

Python bench.py would run benchmark

#   

