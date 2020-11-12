package com.ericsun95.spark.osm.pbf.parser

import java.io.{FileInputStream, FileOutputStream, InputStream, ObjectOutputStream}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PBFFileParserTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  test("Read Entities correctly") {
    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/delaware-latest.osm.pbf"
//    val testFile = "src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.pbf"
    var pbfInputStream: InputStream = null
    val write = true
    if(write) {
      try {
        pbfInputStream = new FileInputStream(testFile)
        val readFile: PBFFileParser = new PBFFileParser(pbfInputStream)
        val out = new ObjectOutputStream(new FileOutputStream("src/test/resources/com/ericsun95/spark/osm/pbf/delaware-latest.osm.txt"))
//        val out = new ObjectOutputStream(new FileOutputStream("src/test/resources/com/ericsun95/spark/osm/pbf/saint-helena-ascension-and-tristan-da-cunha-latest.osm.txt"))
        readFile.foreach(x =>
          out.writeObject(x.toString)
        )
        out.close()
      } finally {
        if (pbfInputStream != null) pbfInputStream.close()
      }
    }
  }

}
