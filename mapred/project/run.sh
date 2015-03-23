#!/bin/bash

hadoop dfs -rmr /project/output > /dev/null 2>&1
hadoop jar target/project-0.0.1-SNAPSHOT.jar wordcount.project.WordCount /project/input /project/output
