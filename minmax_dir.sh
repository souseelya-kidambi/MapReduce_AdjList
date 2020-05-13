#!/bin/bash
hadoop fs -cat /user/kidambsy/soujavatest/dir_outputfinal1/part-r-00000 | sort -k 2 -n -r > dirout1.txt
echo node with Maximum connectivity > finaloutput_dir.txt
sed -n -e '1p' dirout1.txt > dir_temp1.txt
awk -F '\t' '{ print $1 }' dir_temp1.txt >> finaloutput_dir.txt
echo "node with Minimum connectivity" >> finaloutput_dir.txt
sed -n -e '$p' dirout1.txt > dir_temp1.txt
awk -F '\t' '{ print $1 }' dir_temp1.txt >> finaloutput_dir.txt
echo "node with maximum adjacency list" >> finaloutput_dir.txt
sed -n -e '1p' dirout1.txt >> finaloutput_dir.txt



