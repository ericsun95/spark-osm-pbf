package com.ericsun95.spark.osm.pbf.utils

import com.google.protobuf.ByteString
import org.openstreetmap.osmosis.osmbinary.osmformat.{Info, StringTable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DecoderUtilsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with DecoderUtils {

  val stringSeq: Seq[String] = Seq("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "user1", "user2")
  val stringTableSeq: Seq[ByteString] = stringSeq.map(ByteString.copyFrom(_, "UTF-8"))
  val osmosisStringTable: StringTable = StringTable(stringTableSeq)
  val sampleInfo: Info = Info(
    version = Some(1),
    timestamp = Some(1000),
    changeset = Some(0),
    uid = None,
    userSid = Some(8),
    visible = Some(true)
  )
  val optionalSampleInfo: Option[Info] = Some(sampleInfo)

  test("getStringById correctly") {
    for (i <- stringSeq.indices) {
      getStringById(i, osmosisStringTable) shouldBe (stringSeq(i))
    }
  }

  test("getOptional information correctly") {
    getOptionalVersion(optionalSampleInfo) shouldBe Some(1)
    getOptionalChangeset(optionalSampleInfo) shouldBe Some(0)
    getOptionalUid(optionalSampleInfo) shouldBe None
    getOptionalUserSid(optionalSampleInfo) shouldBe Some(8)
    getOptionalUser(optionalSampleInfo, osmosisStringTable) shouldBe Some("user1")
    getOptionalVisible(optionalSampleInfo) shouldBe Some(true)
  }

  test("get cumulative sum correctly") {
    val deltaSeq: Seq[Int] = Seq(0, 1, 1, 1)
    val deltaSeqLong: Seq[Long] = Seq(0L, 1L, 1L, 1L)
    getCumulativeSumInt(deltaSeq) shouldBe Seq(0, 1, 2, 3)
    getCumulativeSumLong(deltaSeqLong) shouldBe Seq(0L, 1L, 2L, 3L)
  }


}
