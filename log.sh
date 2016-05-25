#!/bin/bash

spark-submit --class com.fortysevendeg.log.$1 --files $SPARK_HOME/conf/log4j.properties target/scala-2.10/spark-android-log-assembly-0.0.1.jar
