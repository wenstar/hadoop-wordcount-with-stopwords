wordcount.jar: *.class
	jar -cvf wordcount.jar *.class

*.class: wordcount.java 
	javac -classpath ~/hadoop-1.2.1/hadoop-core-1.2.1.jar wordcount.java
