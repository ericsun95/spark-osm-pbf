package com.ericsun95.spark.osm.pbf.model

import com.ericsun95.spark.osm.pbf.model.OSMElement.OSMRelation.RelationMember
import com.ericsun95.spark.osm.pbf.model.OSMElement.{OSMNode, OSMRelation, OSMWay}
import com.google.protobuf.ByteString
import org.openstreetmap.osmosis.osmbinary.osmformat.{DenseInfo, DenseNodes, Info, Node, Relation, StringTable, Way}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OSMElementTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  //The first element of string table would be empty string
  //TODO: Clean tests by extracting reusable components out
  val stringSeq: Seq[String] = Seq("", "k1", "v1", "k2", "v2", "k3", "v3", "node", "way", "user1", "user2", "inner", "outer")
  val stringTableSeq: Seq[ByteString] = stringSeq.map(ByteString.copyFrom(_, "UTF-8"))
  val osmosisStringTable: StringTable = StringTable(stringTableSeq)
  val sampleInfo: Info = Info(
    version = Some(1),
    timestamp = Some(1000),
    changeset = Some(0),
    uid = None,
    userSid = Some(9),
    visible = Some(true)
  )
  val optionalSampleInfo: Option[Info] = Some(sampleInfo)


  test("OSMNode generated from osmosis node correctly") {
    val osmosisNode: Node = Node(
      1, Seq[Int](1), Seq[Int](2), optionalSampleInfo, 0, 0
    )
    val osmNode = OSMNode(
      osmosisNode = osmosisNode,
      osmosisStringTable = osmosisStringTable,
      latOffset = None,
      lonOffset = None,
      granularity = None,
      dateGranularity = None
    )

    val dummyNode = OSMNode(
      id = 1,
      lat = 0,
      lon = 0,
      tag = Seq[(String, String)](("k1", "v1")),
      version = Some(1),
      timestamp = Some(1000000),
      changeset = Some(0),
      uid = None,
      user = Some("user1"),
      visible = Some(true)
    )
    osmNode shouldBe dummyNode
    osmNode.toString shouldBe dummyNode.toString
  }

  test("OSMNode generated from osmosis dense node correctly") {
    val denseInfo: DenseInfo = DenseInfo(
      version = Seq(0, 1, 1, 1, 1),
      timestamp = Seq(0, 1, 0, 1, -1),
      changeset = Seq(0, 1, 0, 1, -1),
      uid = Seq(0, 1, 0, 1, -1),
      userSid = Seq(9, 1, 0, 0, -1),
      visible = Seq(true, true, true, true, true)
    )

    val osmosisDenseNode: DenseNodes = DenseNodes(
      id  = Seq(0, 1, 1, 1, 1),
      denseinfo  = Some(denseInfo),
      lat = Seq(0, 1, 1, 1, 1),
      lon = Seq(0, 1, 1, 1, 1),
      keysVals = Seq(0, 1, 2, 0, 0, 0, 0)
    )

    val nodesList = OSMNode(osmosisDenseNode, osmosisStringTable, None, None, None, None)
    nodesList(1).shouldBe(
      OSMNode(
        id = 1,
        lat = 1.0000000000000001E-7,
        lon = 1.0000000000000001E-7,
        tag = Seq[(String, String)](("k1", "v1")),
        version = Some(1),
        timestamp = Some(1000),
        changeset = Some(1),
        uid = Some(1),
        user = Some("user2"),
        visible = Some(true)
      )
    )
  }

  test("OSMWay generated from osmosis way correctly") {
    val osmosisWay: Way = Way(
      1, Seq[Int](1), Seq[Int](2), optionalSampleInfo, Seq(1L, 1L, 1L, 1L, -3L), Seq.empty, Seq.empty
    )
    val osmWay = OSMWay(
      osmosisWay,
      osmosisStringTable,
      dateGranularity = None
    )

    val dummyWay = OSMWay(
      id = 1,
      nds = Seq(1L, 2L, 3L, 4L, 1L),
      tag = Seq[(String, String)](("k1", "v1")),
      version = Some(1),
      timestamp = Some(1000000),
      changeset = Some(0),
      uid = None,
      user = Some("user1"),
      visible = Some(true)
    )
    osmWay shouldBe dummyWay
    osmWay.toString shouldBe dummyWay.toString
  }

  test("OSMRelation generated from osmosis relation correctly") {
    val osmosisRelation: Relation = Relation(
      id = 1,
      keys = Seq[Int](1),
      vals = Seq[Int](2),
      info = optionalSampleInfo,
      rolesSid = Seq(11, 12),
      memids = Seq(1, 1),
      types = Seq(Relation.MemberType.WAY, Relation.MemberType.WAY)
    )
    val osmRelation = OSMRelation(
      osmosisRelation,
      osmosisStringTable,
      dateGranularity = None
    )
    val dummyRelation = OSMRelation(
      id = 1,
      relations = Seq(
        RelationMember("way", 1, "inner"),
        RelationMember("way", 2, "outer")
      ),
      tag = Seq[(String, String)](("k1", "v1")),
      version = Some(1),
      timestamp = Some(1000000),
      changeset = Some(0),
      uid = None,
      user = Some("user1"),
      visible = Some(true)
    )
    osmRelation shouldBe dummyRelation
    osmRelation.toString shouldBe dummyRelation.toString
  }

}
