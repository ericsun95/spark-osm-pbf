package com.ericsun95.spark.osm.pbf

import com.ericsun95.spark.osm.pbf.model.OSMInternalRow
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OsmPbfSparkTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  test("OSM PBF Spark DataSource read data correctly") {
    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.pbf"

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val osmInternalRowEncoder: Encoder[OSMInternalRow] = Encoders.product[OSMInternalRow]
    val df = spark.read
      .format("com.ericsun95.spark.osm.pbf.DefaultSource")
      .load(testFile)
      .as[OSMInternalRow](osmInternalRowEncoder)
      .cache()

    df.filter(x => x.osmType == 0).show(5)
    df.filter(x => x.osmType == 1).show(5)
    df.filter(x => x.osmType == 2).show(5)

    assert(df.rdd.getNumPartitions > 1)
    assert(df.rdd.count() == 61217)
    assert(df.filter(x => x.osmType == 0).count() == 56060)
    assert(df.filter(x => x.osmType == 1).count() == 5124)
    assert(df.filter(x => x.osmType == 2).count() == 33)

    spark.stop()
  }
}
