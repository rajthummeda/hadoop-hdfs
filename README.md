# Hadoop-hdfs
Spring boot application connect to Hadoop HDFS

URI - http://localhost:8080/list-files?path=/input
    - http://localhost:8080/read-file?filePath=/input/test.txt

# Adding file 
hadoop fs -put C:\Users\hp\Documents\test.txt /input
hadoop fs -ls /input
