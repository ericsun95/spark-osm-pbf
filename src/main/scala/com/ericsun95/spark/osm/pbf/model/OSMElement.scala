package com.ericsun95.spark.osm.pbf.model

import java.io.Serializable

import org.openstreetmap.osmosis.osmbinary.osmformat.{DenseInfo, DenseNodes, Info, Node, Relation, StringTable, Way}

sealed trait OSMElement extends Product with Serializable {
  val id: Long
  val tag: Seq[(String, String)]
  val version: Option[Int]
  val timestamp: Option[Long]
  val changeset: Option[Long]
  val uid: Option[Int]
  val user: Option[String]
  val visible: Option[Boolean]
}

object OSMElement {

  private val COORDINATE_SCALING_FACTOR = 0.000000001
  private val DEFAULT_LAT_OFFSET = 0L
  private val DEFAULT_LON_OFFSET = 0L
  private val DEFAULT_GRANULARITY = 100
  private val DEFAULT_DATE_GRANULARITY: Int = 1000
  private val DEFAULT_CHARSET: String = "UTF-8"

  def getStringById(id: Int, osmosisStringTable: StringTable): String = {
    osmosisStringTable.s(id).toString(DEFAULT_CHARSET)
  }

  def getOptionalVersion(optionalInfo: Option[Info]): Option[Int] = {
    optionalInfo.filter(_.version.isDefined).map(_.version.get)
  }

  def getOptionalTimeStamp(optionalInfo: Option[Info], dateGranularity: Int): Option[Long] = {
    optionalInfo.filter(_.timestamp.isDefined).map(_.timestamp.get * dateGranularity)
  }

  def getOptionalChangeset(optionalInfo: Option[Info]): Option[Long] = {
    optionalInfo.filter(_.changeset.isDefined).map(_.changeset.get)
  }

  def getOptionalUid(optionalInfo: Option[Info]): Option[Int] = {
    optionalInfo.filter(_.uid.isDefined).map(_.uid.get)
  }

  def getOptionalUserSid(optionalInfo: Option[Info]): Option[Int] = {
    optionalInfo.filter(_.userSid.isDefined).map(_.userSid.get)
  }

  def getOptionalUser(optionalInfo: Option[Info], osmosisStringTable: StringTable): Option[String] = {
    getOptionalUserSid(optionalInfo).map(getStringById(_, osmosisStringTable))
  }

  def getOptionalVisible(optionalInfo: Option[Info]): Option[Boolean] = {
    optionalInfo.filter(_.visible.isDefined).map(_.visible.get)
  }

  def getTags(key: Seq[Int], value: Seq[Int], osmosisStringTable: StringTable): Seq[(String, String)] = {
    (key, value).zipped.map { (k, v) =>
      (getStringById(k, osmosisStringTable), getStringById(v, osmosisStringTable))
    }
  }

  //TODO: Unify this two with Generic Type
  def getCumulativeSumInt(deltaSeq: Seq[Int]): Seq[Int] = {
    deltaSeq.scanLeft(0) {_ + _}.drop(1)
  }

  def getCumulativeSumLong(deltaSeq: Seq[Long]): Seq[Long] = {
    deltaSeq.scanLeft(0L) {_ + _}.drop(1)
  }

  case class OSMNode(id: Long,
                     lat: Double,
                     lon: Double,
                     tag: Seq[(String, String)],
                     version: Option[Int] = None,
                     timestamp: Option[Long] = None,
                     changeset: Option[Long] = None,
                     uid: Option[Int] = None,
                     user: Option[String] = None,
                     visible: Option[Boolean] = None) extends OSMElement

  object OSMNode {
    //TODO: Add support toXML and fromXML

    def apply(osmosisNode: Node,
              osmosisStringTable: StringTable,
              latOffset: Option[Long],
              lonOffset: Option[Long],
              granularity: Option[Int],
              dateGranularity: Option[Int]): OSMNode = {
      val LAT_OFFSET: Long = latOffset.getOrElse(DEFAULT_LAT_OFFSET)
      val LON_OFFSET = lonOffset.getOrElse(DEFAULT_LON_OFFSET)
      val GRANULARITY: Int = granularity.getOrElse(DEFAULT_GRANULARITY)
      val DATE_GRANULARITY: Int = dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY)
      val lat: Double = COORDINATE_SCALING_FACTOR*(LAT_OFFSET + (GRANULARITY*osmosisNode.lat))
      val lon: Double = COORDINATE_SCALING_FACTOR*(LON_OFFSET + (GRANULARITY*osmosisNode.lon))
      val optionalInfo = osmosisNode.info
      OSMNode(
        id = osmosisNode.id,
        lat = lat,
        lon = lon,
        tag = getTags(osmosisNode.keys, osmosisNode.vals, osmosisStringTable),
        version = getOptionalVersion(optionalInfo),
        timestamp = getOptionalTimeStamp(optionalInfo, DATE_GRANULARITY),
        changeset = getOptionalChangeset(optionalInfo),
        uid = getOptionalUid(optionalInfo),
        user = getOptionalUser(optionalInfo, osmosisStringTable),
        visible = getOptionalVisible(optionalInfo)
      )
    }

    def apply(osmosisDenseNode: DenseNodes,
              osmosisStringTable: StringTable,
              latOffset: Option[Long],
              lonOffset: Option[Long],
              granularity: Option[Int],
              dateGranularity: Option[Int]): Seq[OSMNode] = {

      val LAT_OFFSET = latOffset.getOrElse(DEFAULT_LAT_OFFSET)
      val LON_OFFSET = lonOffset.getOrElse(DEFAULT_LON_OFFSET)
      val GRANULARITY = granularity.getOrElse(DEFAULT_GRANULARITY)

      val IdSeq: Seq[Long] = getCumulativeSumLong(osmosisDenseNode.id)
      val latSeq: Seq[Double] = getCumulativeSumLong(osmosisDenseNode.lat).map(lat => {
        COORDINATE_SCALING_FACTOR*(LAT_OFFSET + GRANULARITY*lat)
      })
      val lonSeq: Seq[Double] = getCumulativeSumLong(osmosisDenseNode.lon).map(lon => {
        COORDINATE_SCALING_FACTOR*(LON_OFFSET + GRANULARITY*lon)
      })

      val tagsIterator: Iterator[Int] = osmosisDenseNode.keysVals.iterator
      var tagsSeq: Seq[Seq[(String, String)]] = Seq[Seq[(String, String)]]()
      while(tagsIterator.hasNext) {
        val tag = tagsIterator.takeWhile(_.equals(0L)).grouped(2).map(tag =>
          (getStringById(tag.head, osmosisStringTable), getStringById(tag.last, osmosisStringTable))
        )
        tagsSeq ++ tag
      }

      val denseInfoCollection: Option[DenseInfo] = osmosisDenseNode.denseinfo
      val versionSeq: Option[Seq[Int]] = denseInfoCollection.map(info => getCumulativeSumInt(info.version))
      val timestampSeq: Option[Seq[Long]] = denseInfoCollection.map(
        x => getCumulativeSumLong(x.timestamp).map(_ * dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY))
      )
      val changesetSeq: Option[Seq[Long]] = denseInfoCollection.filter(_.changeset.nonEmpty).map(info => getCumulativeSumLong(info.changeset))
      val uidSeq: Option[Seq[Int]] = denseInfoCollection.map(info => getCumulativeSumInt(info.uid))
      val userSidSeq: Option[Seq[Int]] = denseInfoCollection.map(info => getCumulativeSumInt(info.userSid))
      val userSeq: Option[Seq[String]] = userSidSeq.map(x => x.map(getStringById(_, osmosisStringTable)))
      val visibleSeq: Option[Seq[Boolean]] = denseInfoCollection.map(info => info.visible)

      val len = IdSeq.size
      (0 until len) map {
        i => OSMNode(
          IdSeq(i),
          latSeq(i),
          lonSeq(i),
          tagsSeq(i),
          versionSeq.map(_(i)),
          timestampSeq.map(_(i)),
          changesetSeq.map(_(i)),
          uidSeq.map(_(i)),
          userSeq.map(_(i)),
          visibleSeq.map(_(i))
        )
      }

    }
  }

  case class OSMWay(id: Long,
                    nds: Seq[Long],
                    tag: Seq[(String, String)],
                    version: Option[Int] = None,
                    timestamp: Option[Long] = None,
                    changeset: Option[Long] = None,
                    uid: Option[Int] = None,
                    user: Option[String] = None,
                    visible: Option[Boolean] = None) extends OSMElement

  object OSMWay {

    //TODO: Add support toXML and fromXML
    def apply(osmosisWay: Way,
              osmosisStringTable: StringTable,
              dateGranularity: Option[Int]): OSMWay = {

      val DATE_GRANULARITY: Int = dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY)
      val nodes: Seq[Long] = getCumulativeSumLong(osmosisWay.refs)
      val optionalInfo: Option[Info] = osmosisWay.info

      OSMWay(
        id = osmosisWay.id,
        nds = nodes,
        tag = getTags(osmosisWay.keys, osmosisWay.vals, osmosisStringTable),
        version = getOptionalVersion(optionalInfo),
        timestamp = getOptionalTimeStamp(optionalInfo, DATE_GRANULARITY),
        changeset = getOptionalChangeset(optionalInfo),
        uid = getOptionalUid(optionalInfo),
        user = getOptionalUser(optionalInfo, osmosisStringTable),
        visible = getOptionalVisible(optionalInfo)
      )
    }
  }

  case class OSMRelation(id: Long,
                         relations: Seq[RelationMember],
                         tag: Seq[(String, String)],
                         version: Option[Int] = None,
                         timestamp: Option[Long] = None,
                         changeset: Option[Long] = None,
                         uid: Option[Int] = None,
                         user: Option[String] = None,
                         visible: Option[Boolean] = None) extends OSMElement

  object OSMRelation {

    //TODO: Add support toXML and fromXML
    def getRelationMember(ids: Seq[Long], types: Seq[Relation.MemberType],
                          rolesSids: Seq[Int], osmosisStringTable: StringTable): Seq[RelationMember] = {
      (ids, types, rolesSids).zipped.map(
        (id, typeVal, rolesSid) =>
          RelationMember(typeVal.name.toLowerCase, id, OSMElement.getStringById(rolesSid, osmosisStringTable))
      )
    }

    def apply(osmosisRelation: Relation,
              osmosisStringTable: StringTable,
              dateGranularity: Option[Int]): OSMRelation = {

      val DATE_GRANULARITY: Int = dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY)

      // Decode members references in stored in delta compression.
      val members: Seq[Long] = getCumulativeSumLong(osmosisRelation.memids)
      val relations: Seq[RelationMember] = getRelationMember(members,
        osmosisRelation.types, osmosisRelation.rolesSid, osmosisStringTable)
      val optionalInfo: Option[Info] = osmosisRelation.info

      OSMRelation(
        id = osmosisRelation.id,
        relations = relations,
        tag = getTags(osmosisRelation.keys, osmosisRelation.vals, osmosisStringTable),
        version = getOptionalVersion(optionalInfo),
        timestamp = getOptionalTimeStamp(optionalInfo, DATE_GRANULARITY),
        changeset = getOptionalChangeset(optionalInfo),
        uid = getOptionalUid(optionalInfo),
        user = getOptionalUser(optionalInfo, osmosisStringTable),
        visible = getOptionalVisible(optionalInfo)
      )
    }
  }
}

case class RelationMember(`type`: String, ref: Long, role: String)


