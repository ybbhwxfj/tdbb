#!/bin/bash
# To generate core dump in docker container, the host needs
# to be configured to save the dumps in a certain location
echo '/tmp/core.%t.%e.%p' | sudo tee /proc/sys/kernel/core_pattern