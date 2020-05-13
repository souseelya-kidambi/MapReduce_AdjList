#!/bin/bash
hadoop fs -cat /user/kidambsy/soujavatest/outputfinal3/part-r-00000 | sort -k 2 -n -r > dipartfin1.txt
echo node with Maximum connectivity > finaloutput.txt
sed -n -e '1p' dipartfin1.txt > temp1.txt
awk -F '\t' '{ print $1 }' temp1.txt >> finaloutput.txt
echo "node with Minimum connectivity" >> finaloutput.txt
sed -n -e '$p' dipartfin1.txt > temp1.txt
awk -F '\t' '{ print $1 }' temp1.txt >> finaloutput.txt
echo "node with maximum adjacency list" >> finaloutput.txt
sed -n -e '1p' dipartfin1.txt >> finaloutput.txt



