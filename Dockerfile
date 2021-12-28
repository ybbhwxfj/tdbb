FROM block-dev:latest AS block-db

ENV DEBIAN_FRONTEND noninteractive

RUN rm -rf /root/.ssh
RUN ssh-keygen -q -t rsa -N '' -f /root/.ssh/id_rsa
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

COPY . /root/block-db

RUN echo 'Asia/Shanghai' > /etc/timezone
RUN ln -s -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN rm /etc/security/limits.conf && \
    echo 'root    soft    nofile  50000 ' >> /etc/security/limits.conf && \
    echo 'root    hard    nofile  50000 ' >> /etc/security/limits.conf && \
    echo 'root    soft    core  unlimited ' >> /etc/security/limits.conf && \
    echo 'root    hard    core  unlimited ' >> /etc/security/limits.conf

WORKDIR /root/block-db

RUN cd /root/block-db && \
    mkdir -p build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
    make -j

ENTRYPOINT service ssh restart && bash
