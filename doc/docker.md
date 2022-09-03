# docker command

## delete dangling images

    docker rmi -f $(docker images -f "dangling=true" -q)

## docker-compose

    docker-compose  -f conf/docker-compose.sdb.b.yml up -d