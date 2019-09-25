#!/bin/bash
list_tables=/home/hdfs/show_num_parition/tables.txt
list_partitions=/home/hdfs/output.txt
number_of_partition=/home/hdfs/statistics_partitions.txt 

# dump list tables into tables.txt
hive --showHeader=false --outputformat=tsv2 -f /home/hdfs/show_num_parition/command.sql > $list_tables

# dump list partitions into output.txt
rm -rf $list_partitions
while read line
do
	echo "$line" >> $list_partitions
	eval "hive --showHeader=false --outputformat=tsv2  -e 'show partitions movie_len.$line'" >> $list_partitions
	echo "----" >> $list_partitions
done < "$list_tables"

# statistics number of partitions in tables
count_partition=0
while read line
do
	if(($count_partition==0))
	then
		echo "$line" >> number_of_partition
	fi
	count_partition=$((count_partition + 1))
	if(($line=='----'))
	then
		echo $((count_partition -1)) >> number_of_partition
		count_partition=0 
	fi

done < "$list_partitions"
