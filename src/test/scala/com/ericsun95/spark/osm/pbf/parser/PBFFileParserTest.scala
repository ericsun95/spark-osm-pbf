package com.ericsun95.spark.osm.pbf.parser

import java.io.{FileInputStream, InputStream}

import com.ericsun95.spark.osm.pbf.model.OSMElement
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PBFFileParserTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  test("PBFFileParser parse osm pbf correctly") {
    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.pbf"
    var pbfInputStream: InputStream = null
    try {
      pbfInputStream = new FileInputStream(testFile)
      val readFile: PBFFileParser = new PBFFileParser(pbfInputStream)
      var nodeNumber = 0
      var wayNumber = 0
      var relationNumber = 0
      readFile.foreach {
        case OSMElement.OSMNode(id, lat, lon, tag, version, timestamp, changeset, uid, user, visible) => nodeNumber += 1
        case OSMElement.OSMWay(id, nds, tag, version, timestamp, changeset, uid, user, visible) => wayNumber += 1
        case OSMElement.OSMRelation(id, relations, tag, version, timestamp, changeset, uid, user, visible) => relationNumber += 1
      }
      assert(nodeNumber == 56060)
      assert(wayNumber == 5124)
      assert(relationNumber == 33)
    } finally {
      if (pbfInputStream != null) pbfInputStream.close()
    }
  }

//  test("Read Entities correctly") {
//    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/delaware-latest.osm.pbf"
////    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.pbf"
//    var pbfInputStream: InputStream = null
//    val write = false
//    if(write) {
//      try {
//        pbfInputStream = new FileInputStream(testFile)
//        val readFile: PBFFileParser = new PBFFileParser(pbfInputStream)
//        val out = new ObjectOutputStream(new FileOutputStream("src/test/resources/com/ericsun95/spark/osm/pbf/delaware-latest.osm.txt"))
////        val out = new ObjectOutputStream(new FileOutputStream("src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.txt"))
//        readFile.foreach(x =>
//          out.writeObject(x.toString)
//        )
//        out.close()
//      } finally {
//        if (pbfInputStream != null) pbfInputStream.close()
//      }
//    }
//  }

}
