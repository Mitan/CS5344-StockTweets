rm -rf output/
rm ReverseGraph.jar
rm classes/*.*
javac -classpath `hadoop classpath` -d classes/ ReverseGraph.java
jar -cvf ReverseGraph.jar -C classes/ .
hadoop jar ReverseGraph.jar ReverseGraph
hdfs dfs -get /output .

