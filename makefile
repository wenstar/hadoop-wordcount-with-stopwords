wordcount.jar: wordcount.class wordsort.class
	jar -cvf wordcount.jar *.class

wordcount.class: wordcount.java 
	javac -classpath ~/hadoop-1.2.1/hadoop-core-1.2.1.jar wordcount.java
wordsort.class: wordsort.java
	javac -classpath ~/hadoop-1.2.1/hadoop-core-1.2.1.jar wordsort.java
