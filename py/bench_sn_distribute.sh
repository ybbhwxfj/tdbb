#!/bin/bash
nohup python3 bench.py  -dt -t sn -tp distribute > fe.out 2>&1 &
