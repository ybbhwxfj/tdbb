#!/bin/bash
export PATH=${PATH}:/home/mysql/block-db/bin
echo mysql | sudo -S service ssh restart
bash
