#!/bin/bash
nohup python3 bench.py -t tb -tp readonly > fe.out 2>&1 &
