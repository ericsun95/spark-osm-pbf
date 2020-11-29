package com.ericsun95.spark.osm.pbf.parser

import java.io.{FileInputStream, InputStream}

import com.ericsun95.spark.osm.pbf.model.OSMElement
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob

class PBFFileParserTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.pbf"
  var pbfInputStream: InputStream = null

  test("PBFFileParser parse osm pbf correctly") {
    try {
      pbfInputStream = new FileInputStream(testFile)
      val pbfFileParser: PBFFileParser = new PBFFileParser(pbfInputStream)
      var nodeNumber = 0
      var wayNumber = 0
      var relationNumber = 0
      pbfFileParser.foreach {
        case n: OSMElement.OSMNode => nodeNumber += 1
        case w: OSMElement.OSMWay => wayNumber += 1
        case r: OSMElement.OSMRelation => relationNumber += 1
      }
      nodeNumber shouldBe 56060
      wayNumber shouldBe 5124
      relationNumber shouldBe 33
    } finally {
      if (pbfInputStream != null) {
        pbfInputStream.close()
        pbfInputStream = null
      }
    }
  }

  test("PBFFileParser parse osm pbf blob correctly") {
    try {
      pbfInputStream = new FileInputStream(testFile)
      val pbfFileParser: PBFFileParser = new PBFFileParser(pbfInputStream)
      val pbfBlobIterator: Iterator[Blob] = pbfFileParser.pbfBlobIterator
      val firstPbfBlobParser = new PBFBlobParser(pbfBlobIterator.next())
      pbfBlobIterator.size shouldBe 9
      firstPbfBlobParser.size shouldBe 8000
    } finally {
      if (pbfInputStream != null) {
        pbfInputStream.close()
        pbfInputStream = null
      }
    }
  }

}
