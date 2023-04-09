# Generate configure files

File az1.txt, az2.txt, az2.txt would be read by configure.py to generate configure files.
The priority to become new leader, az3 > az2 > az3.

Ex1: Generate DB Shared , tight bind

    python3 configure.py -s 1 -tb -p ./az_sdb_tight

Ex2: Generate DB Shared CCB and RLB, Scale DSB by 5 shards

    python3 configure.py -s 1 -s -p ./az_scrdb

#### command line of configure.py:

    usage: configure.py [-h] [-p PATH] [-s SHARDS] [-tb] [-scr]

    configure

    options:
    -h, --help            show this help message and exit
    -p PATH, --path PATH  az configure file path
    -s SHARDS, --shards SHARDS
    -tb, --tight-bind     tight bind
    -scr, --share-ccb-rlb
                            share CCB and RLB, and scale DSB block

# Compute line of code

Python mloc.py would compute blocks' lines of code

# Run benchmark

Python bench.py would run benchmark

#### command line of bench.py : 

```
usage: bench.py [-h] [-lw CONFIGURE_LATENCY_WAN] [-ll CONFIGURE_LATENCY_LAN] [-bw CONFIGURE_BANDWIDTH_WAN] [-mg] [-ml]
                [-mi] [-mr] [-t DB_CONFIG_TYPE] [-r RUN_COMMAND] [-c] [-tp TEST_PARAMETER] [-dt] [-dg DEBUG_URL]

benchmark

options:
  -h, --help            show this help message and exit
  -lw CONFIGURE_LATENCY_WAN, --configure-latency-wan CONFIGURE_LATENCY_WAN
                        configure wan latency(ms)
  -ll CONFIGURE_LATENCY_LAN, --configure-latency-lan CONFIGURE_LATENCY_LAN
                        configure lan latency(ms)
  -bw CONFIGURE_BANDWIDTH_WAN, --configure-bandwidth-wan CONFIGURE_BANDWIDTH_WAN
                        configure wan bandwidth(Mbps)
  -mg, --mysql-data-gen
                        test data generate
  -ml, --mysql-load     test mysql load
  -mi, --mysql-init     test mysql load
  -mr, --mysql-run      test mysql run
  -t DB_CONFIG_TYPE, --db-config-type DB_CONFIG_TYPE
                        db config type:lb/tb/sn/scr
  -r RUN_COMMAND, --run-command RUN_COMMAND
                        run command on all site
  -c, --clean           clean all
  -tp TEST_PARAMETER, --test-parameter TEST_PARAMETER
                        test parameter:cache/readonly/terminal/distribute
  -dt, --distributed-tx
                        control remote distributed transaction
  -dg DEBUG_URL, --debug-url DEBUG_URL
                        debug url
```