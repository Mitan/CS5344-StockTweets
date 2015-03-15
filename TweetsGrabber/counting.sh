#!/bin/bash
echo "Counting number of tweets in $1 "
declare -a arr=("2015-03-07" "2015-03-08" "2015-03-09" "2015-03-10" "2015-03-11" "2015-03-12" "2015-03-13" "2015-03-14" "2015-03-15")

## now loop through the above array
for i in "${arr[@]}"
do
   echo "Counting tweets of date: $i"
   cat $1 | grep $i | wc -l
   # or do whatever with individual element of the array
done
