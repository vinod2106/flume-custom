#!/bin/sh

echo "Please enter the size in terms of rows you want to generate the random file data"
read rows
#rows=$1

echo "Please enter the delay time needed in between the writing of the rows"
read delayTime

#Delete the tmp & generatedRandomDataFile files if they already exist
rm -f tmp
rm -f generatedRandomDataFile

start=$(date +%s)
for i in $(seq $rows)
do
tr -dc 0-9 < /dev/urandom | head -c 2 > tmp
gawk '$1=$1' tmp >> generatedRandomDataFile
sleep $delayTime
done
end=$(date +%s)
DIFF=$(( $end - $start ))
echo "File generated is `pwd`/generatedRandomDataFile"
echo "The file generation took $DIFF seconds"