#!/bin/bash
nohup python3 bench.py -t lb -tp cache > fe.out 2>&1 &
