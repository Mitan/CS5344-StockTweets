rm -rf output/
rm Pagerank.jar
rm classes/*.*
hdfs dfs -rmr /output
javac -classpath `hadoop classpath` -d classes/ Pagerank.java
jar -cvf Pagerank.jar -C classes/ .
hadoop jar Pagerank.jar Pagerank
hdfs dfs -get /output .

