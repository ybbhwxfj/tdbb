dir=$(dirname "$0")
project_directory=$(cd "$dir" || exit; cd ..; pwd)

apt-get update && \
    apt-get -y dist-upgrade && \
    apt-get install -y \
    software-properties-common \
    wget \
    make \
    g++ \
    cmake \
    zlib1g \
    zlib1g-dev \
    libsnappy-dev \
    libzstd-dev \
    libbz2-dev \
    liblz4-dev \
    libgflags-dev \
    liburing-dev \
    libssl-dev \
    libgflags-dev \
    libreadline-dev \
    git \
    vim \
    openssh-client \
    openssh-server \
    openssh-sftp-server \
    python3 \
    python3-pip \
    net-tools \
    iputils-ping \
    iproute2 \
    rsync \
    gdb


pip3 install \
    paramiko

echo 'Asia/Shanghai' > /etc/timezone

echo 'root    soft    nofile  50000 ' >> /etc/security/limits.conf && \
echo 'root    hard    nofile  50000 ' >> /etc/security/limits.conf && \
echo 'root    soft    core  unlimited ' >> /etc/security/limits.conf && \
echo 'root    hard    core  unlimited ' >> /etc/security/limits.conf


# prerequisite for mysql
userdel -r -f mysql
groupadd -r -f mysql
groupadd -r -f mysql && useradd -r  -m -g mysql mysql

echo "mysql:25892710@mysql" | chpasswd
rm -rf /var/log/mysql
mkdir -p /var/log/mysql
chown mysql:mysql /var/log/mysql
usermod -aG sudo mysql

apt-get install -y bison \
    libncurses-dev \
    libldap-dev \
    libgsasl-dev \
    libsasl2-dev

echo "PATH=${PATH}:/home/mysql/block-db/bin" > /etc/profile


# set rsa key
ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys

# build third party library


bash "${project_directory}/script/build_ssl.sh"
bash "${project_directory}/script/build_mysql.sh"
bash "${project_directory}/script/build_third_party.sh" -i /usr
