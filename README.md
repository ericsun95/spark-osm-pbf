# OSM PBF Data Source for Apache Spark

- A library for parsing and querying [OSM PBF](https://wiki.openstreetmap.org/wiki/PBF_Format) data with Apache Spark, for Spark SQL and DataFrames.
- This package support processing [OSM PBF](https://wiki.openstreetmap.org/wiki/PBF_Format) File directly through DataInputStream or distributed through apache spark.
- This package extends [DataSourceV2](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-DataSourceV2.html) to take advantage of fast processing and push down/fliter optimization.
- This package support reading as type safe Dataset ``Dataset[OSMInternalRow]`` to take advantage of spark Dataset optimization.

# Version Requirements
Currently support scala `2.11` and spark `2.4.x`.

