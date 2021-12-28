#!/bin/bash
nohup python3 bench.py -l 50 -t unbind > fe.out 2>&1 &
