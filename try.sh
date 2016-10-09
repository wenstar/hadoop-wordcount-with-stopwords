make
~/hadoop-1.2.1/bin/hadoop fs -rmr output
~/hadoop-1.2.1/bin/hadoop fs -rmr stop-words
~/hadoop-1.2.1/bin/hadoop fs -put stop-words stop-words
~/hadoop-1.2.1/bin/hadoop jar wordcount.jar  wordcount -min 4 input output
rm -r ~/Desktop/output
~/hadoop-1.2.1/bin/hadoop fs -get output ~/Desktop
