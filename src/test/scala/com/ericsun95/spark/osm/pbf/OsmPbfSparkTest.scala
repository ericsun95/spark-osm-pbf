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

    assert(df.rdd.getNumPartitions > 1)
    assert(df.rdd.count() == 61217)

    val df_nodes = df.filter(x => x.osmType == 0).cache()
    val df_ways =df.filter(x => x.osmType == 1).cache()
    val df_relations =df.filter(x => x.osmType == 2).cache()

    df_nodes.show(5)
    df_ways.show(5)
    df_relations.show(5)

    assert(df_nodes.count() == 56060)
    assert(df_ways.count() == 5124)
    assert(df_relations.count() == 33)

    spark.stop()
  }

//  test("OSM PBF Spark DataSource read big data correctly") {
//    val testFile = "/Users/eric_sun/Desktop/asia-latest.osm.pbf"
//
//    val spark = SparkSession
//      .builder()
//      .master("local[4]")
//      .config("spark.executor.memory", "4g")
//      .config("spark.driver.memory", "4g")
//      .getOrCreate()
//
//    val osmInternalRowEncoder: Encoder[OSMInternalRow] = Encoders.product[OSMInternalRow]
//    val df = spark.read
//      .format("com.ericsun95.spark.osm.pbf.DefaultSource")
//      .load(testFile)
//      .as[OSMInternalRow](osmInternalRowEncoder)
//
//    df.show(10)
//
//    spark.stop()
//  }
}
