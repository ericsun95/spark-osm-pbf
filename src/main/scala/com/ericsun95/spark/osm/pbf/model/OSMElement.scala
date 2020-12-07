package com.ericsun95.spark.osm.pbf.model

import java.io.Serializable

import com.ericsun95.spark.osm.pbf.model.OSMElement.OSMRelation.RelationMember
import com.ericsun95.spark.osm.pbf.utils.DecoderUtils
import org.openstreetmap.osmosis.osmbinary.osmformat._

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

object OSMElement extends Serializable with DecoderUtils {

  case class OSMNode(id: Long,
                     lat: Double,
                     lon: Double,
                     tag: Seq[(String, String)],
                     version: Option[Int] = None,
                     timestamp: Option[Long] = None,
                     changeset: Option[Long] = None,
                     uid: Option[Int] = None,
                     user: Option[String] = None,
                     visible: Option[Boolean] = None) extends OSMElement {

    //TODO: Update this to String in geoJSON
    override def toString: String = {
      s"Node id: ${id}, " +
        s"coordinate: (${lat}, ${lon}), " +
        s"tags: ${tag}, " +
        s"version: ${version.getOrElse("None")}," +
        s"timestamp: ${timestamp.getOrElse("None")}, " +
        s"changeset: ${changeset.getOrElse("None")}, " +
        s"uid: ${uid.getOrElse("None")}, " +
        s"user: ${user.getOrElse("None")}, " +
        s"visible: ${visible.getOrElse("None")}\n"
    }
  }


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

      val IdSeq: Seq[Long] = getCumulativeSum(osmosisDenseNode.id)
      val latSeq: Seq[Double] = getCumulativeSum(osmosisDenseNode.lat).map(lat => {
        COORDINATE_SCALING_FACTOR*(LAT_OFFSET + GRANULARITY*lat)
      })
      val lonSeq: Seq[Double] = getCumulativeSum(osmosisDenseNode.lon).map(lon => {
        COORDINATE_SCALING_FACTOR*(LON_OFFSET + GRANULARITY*lon)
      })

      val tagsIterator = osmosisDenseNode.keysVals.iterator
      var tagsSeq = List[Seq[(String, String)]]()
      while(tagsIterator.hasNext) {
        val tagSeq = tagsIterator.takeWhile(_ != 0L).grouped(2).map(pair =>
          (getStringById(pair.head, osmosisStringTable), getStringById(pair.last, osmosisStringTable))
        ).toList
        tagsSeq = tagsSeq :+ tagSeq
      }

      assert(tagsSeq.size == IdSeq.size)
      assert(IdSeq.size == latSeq.size)
      assert(IdSeq.size == lonSeq.size)

      val denseInfoCollection: Option[DenseInfo] = osmosisDenseNode.denseinfo
      val versionSeq: Option[Seq[Int]] = denseInfoCollection.filter(_.version.nonEmpty).map(info => info.version)
      val timestampSeq: Option[Seq[Long]] = denseInfoCollection.map(
        x => getCumulativeSum(x.timestamp).map(_ * dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY))
      )
      val changesetSeq: Option[Seq[Long]] = denseInfoCollection.filter(_.changeset.nonEmpty).map(info => getCumulativeSum(info.changeset))
      val uidSeq: Option[Seq[Int]] = denseInfoCollection.filter(_.uid.nonEmpty).map(info => getCumulativeSum(info.uid))
      val userSidSeq: Option[Seq[Int]] = denseInfoCollection.filter(_.userSid.nonEmpty).map(info => getCumulativeSum(info.userSid))
      val userSeq: Option[Seq[String]] = userSidSeq.map(x => x.map(getStringById(_, osmosisStringTable)))
      val visibleSeq: Option[Seq[Boolean]] = denseInfoCollection.filter(_.visible.nonEmpty).map(info => info.visible)

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
                    visible: Option[Boolean] = None) extends OSMElement {
    override def toString: String = {
      s"Way id: ${id}, " +
        s"nodes: ${nds}, " +
        s"tags: ${tag}, " +
        s"version: ${version.getOrElse("None")}," +
        s"timestamp: ${timestamp.getOrElse("None")}, " +
        s"changeset: ${changeset.getOrElse("None")}, " +
        s"uid: ${uid.getOrElse("None")}, " +
        s"user: ${user.getOrElse("None")}, " +
        s"visible: ${visible.getOrElse("True")}\n"
    }
  }

  object OSMWay {

    //TODO: Add support toXML and fromXML
    def apply(osmosisWay: Way,
              osmosisStringTable: StringTable,
              dateGranularity: Option[Int]): OSMWay = {

      val DATE_GRANULARITY: Int = dateGranularity.getOrElse(DEFAULT_DATE_GRANULARITY)
      val nodes: Seq[Long] = getCumulativeSum(osmosisWay.refs)
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
                         visible: Option[Boolean] = None) extends OSMElement {
    override def toString: String = {
      s"Relation id: ${id}, " +
        s"relations: ${relations}, " +
        s"tags: ${tag}, " +
        s"version: ${version.getOrElse("None")}," +
        s"timestamp: ${timestamp.getOrElse("None")}, " +
        s"changeset: ${changeset.getOrElse("None")}, " +
        s"uid: ${uid.getOrElse("None")}, " +
        s"user: ${user.getOrElse("None")}, " +
        s"visible: ${visible.getOrElse("None")}\n"
    }
  }

  object OSMRelation {

    case class RelationMember(`type`: String, ref: Long, role: String)

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
      val members: Seq[Long] = getCumulativeSum(osmosisRelation.memids)
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


