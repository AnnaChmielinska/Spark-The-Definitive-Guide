from pyspark.sql.functions import spark_partition_id, asc, desc, max, min

sc.defaultParallelism # 6
spark.conf.get("spark.sql.shuffle.partitions") # 200

df = spark.range(2, 10000000, 2)

df.rdd.getNumPartitions() # 6

stat = df\
    .withColumn("partitionId", spark_partition_id())\
    .groupBy("partitionId")\
    .count()

stat.show()

stat.select(max("count")).show()

stat.select(min("count")).show()

