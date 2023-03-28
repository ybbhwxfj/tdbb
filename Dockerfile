FROM block-dev:latest AS block-db

ENV DEBIAN_FRONTEND noninteractive

RUN rm -rf /root/.ssh
RUN ssh-keygen -q -t rsa -N '' -f /root/.ssh/id_rsa
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

RUN rm -rf /root/block-db

COPY . /root/block-db

RUN echo 'root:root' | chpasswd
RUN echo '{\n\
    "user": "root",\n\
    "password": "root"\n\
}' > /root/block-db/conf/user.conf.json

RUN cp /root/block-db/conf/docker.node.conf.sndb.json /root/block-db/conf/node.conf.sndb.json
RUN cp /root/block-db/conf/docker.node.conf.sdb.lb.json /root/block-db/conf/node.conf.sdb.lb.json
RUN cp /root/block-db/conf/docker.node.conf.sdb.tb.json /root/block-db/conf/node.conf.sdb.tb.json
RUN cp /root/block-db/conf/docker.node.conf.scrdb.json /root/block-db/conf/node.conf.scrdb.json
RUN cp /root/block-db/conf/docker.node.conf.sdb.tb.json /root/block-db/conf/node.mysql.conf.json

RUN echo 'Asia/Shanghai' > /etc/timezone
RUN ln -s -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN rm /etc/security/limits.conf && \
    echo 'root    soft    nofile  50000 ' >> /etc/security/limits.conf && \
    echo 'root    hard    nofile  50000 ' >> /etc/security/limits.conf && \
    echo 'root    soft    core  unlimited ' >> /etc/security/limits.conf && \
    echo 'root    hard    core  unlimited ' >> /etc/security/limits.conf

WORKDIR /root/block-db

RUN rm -rf /root/block-db/bin /root/block-db/build
RUN cd /root/block-db && \
    mkdir -p build && \
    cd build && \
    cmake -DDISABLE_TEST=true -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
    make -j


ENTRYPOINT service ssh restart && bash
