make
../bin/hadoop fs -rmr output
../bin/hadoop fs -rmr stop-words
../bin/hadoop fs -put stop-words stop-words
../bin/hadoop jar wordcount.jar  wordcount -min 4 input output
rm -r ~/Desktop/output
../bin/hadoop fs -get output ~/Desktop
