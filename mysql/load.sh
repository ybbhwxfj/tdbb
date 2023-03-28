export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/:/usr/local/lib/
DBNAME=$1
WH=$2
USER=$3
PASSWORD=$4
HOST=$5
STEP=100

tpcc_mysql_load -h $HOST -d $DBNAME -u $USER -p$PASSWORD -w $WH -l 1 -m 1 -n $WH >> 1.out &

x=1

while [ $x -le $WH ]
do
  echo $x $(( $x + $STEP - 1 ))
  tpcc_mysql_load -h $HOST -d $DBNAME -u $USER -p$PASSWORD -w $WH -l 2 -m $x -n $(( $x + $STEP - 1 ))  >> 2_$x.out &
  tpcc_mysql_load -h $HOST -d $DBNAME -u $USER -p$PASSWORD -w $WH -l 3 -m $x -n $(( $x + $STEP - 1 ))  >> 3_$x.out &
  tpcc_mysql_load -h $HOST -d $DBNAME -u $USER -p$PASSWORD -w $WH -l 4 -m $x -n $(( $x + $STEP - 1 ))  >> 4_$x.out &
  x=$(( $x + $STEP ))
done