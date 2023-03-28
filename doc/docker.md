# configure docker

## Docker [post install](https://docs.docker.com/engine/install/linux-postinstall/)

## Build Docker Images

build block-dev image:

    docker build -t block-dev -f Dockerfile.dev .

build block-db image:

    docker build -t block-db -f Dockerfile .

## Run Docker Container

shared-database tight binding

    docker-compose -f conf/docker-compose.sdb.tb.yml up -d 

shared-database loosely binding

    docker-compose -f conf/docker-compose.sdb.lb.yml up -d 

shared-nothing database

    docker-compose -f conf/docker-compose.sndb.yml up -d 

# Some Docker commands

## delete dangling images

    docker rmi -f $(docker images -f "dangling=true" -q)

## docker remove

    docker rm -f $(docker ps -aq)

## docker-compose

    docker-compose -f conf/docker-compose.sdb.b.yml up -d

## docker exec

    docker exec -ti conf-bdb-client-az3-1 bash
