#TPCC benchmark for MySQL.

Porting from Percona-Lab:

    git@github.com:Percona-Lab/tpcc-mysql.git

# Minimum MySQL version :

MySQL 8.0.23

# Disabled AppArmor for MySQL on Ubuntu:

    sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
    cd /etc/apparmor.d/disable
    sudo ln -s /etc/apparmor.d/usr.sbin.mysqld .


# mysql TPCC test
## create mysql user

## setting configure file

editor user.conf.json

    {
        "user": "mysql",
        "password": "mysql"
    }

## run benchmark
    
generate data:

    bench_mysql_gen.sh

load data:

    bench_mysql_load.sh

run bench:

    bench_mysql_run.sh