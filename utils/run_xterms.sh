#!/bin/bash
#
MACHINE=`cat /espace/Auber_PLE-005/hosts`
C=0
TOTAL=$(cat hosts | wc -l)
TOTAL=$(($TOTAL - 1))
for IP in $MACHINE
do
 echo "[Test]: Computer " $IP
nohup ssh -X -o ConnectTimeout=1 $IP "nohup xterm&"& > res.txt;
 TEST=$(cat res.txt | wc -l);
 if [ $TEST -gt 0 ]
 then
     echo "[OK]"
     C=$(($C + 1))
 else
  echo "[not OK]"
 fi
 echo $C "/" $TOTAL " computers available"
done
