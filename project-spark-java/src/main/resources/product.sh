#!/bin/bash

SPARK_HOME=/opt/modules/spark-1.6.1-bin-2.5.0-cdh5.3.6

${SPARK_HOME}/bin/spark-submit \
	--master yarn-cluster \
	--num-executors 100 \
	--executor-memory 1G \
	--executor-cores 4 \
	--driver-memory 1G \
	/opt/jars/spark/product.jar 3
