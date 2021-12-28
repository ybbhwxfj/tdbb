#!/bin/bash
echo '/tmp/core.%t.%e.%p' | sudo tee /proc/sys/kernel/core_pattern