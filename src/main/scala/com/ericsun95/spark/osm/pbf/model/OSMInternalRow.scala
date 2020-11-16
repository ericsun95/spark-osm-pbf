package com.ericsun95.spark.osm.pbf.model

import com.ericsun95.spark.osm.pbf.model.OSMElement.OSMRelation.RelationMember


/**
 * This is a initial version of OSMInternalRow
 * @param id osm element id
 * @param lat osm element lat
 * @param lon osm element lon
 * @param nds osm element node list
 * @param relations osm element relation member sequence
 * @param tag osm element tags
 * @param version osm element version
 * @param timestamp osm element timestamp
 * @param changeset osm element changeset
 * @param uid osm element uid
 * @param user osm element user
 * @param visible osm element visible
 * @param osmType osm element type
 */
case class OSMInternalRow(id: Long,
                          lat: Option[Double],
                          lon: Option[Double],
                          nds: Seq[Long],
                          relations: Seq[RelationMember],
                          tag: Seq[(String, String)],
                          version: Option[Int],
                          timestamp: Option[Long],
                          changeset: Option[Long],
                          uid: Option[Int],
                          user: Option[String],
                          visible: Option[Boolean],
                          osmType: Byte)

object OSMInternalRow {

  /**
   * Generate OSMInternalRow from OSMElement
   * @param osmElement osm element (node/way/relation)
   * @return OSMInternalRow
   */
  def apply(osmElement: OSMElement): OSMInternalRow = {
    osmElement match {
      case OSMElement.OSMNode(id, lat, lon, tag, version, timestamp, changeset, uid, user, visible) =>
        OSMInternalRow(
          id = id,
          lat = Some(lat),
          lon = Some(lon),
          nds = Seq.empty,
          relations = Seq.empty,
          tag = tag,
          version = version,
          timestamp = timestamp,
          changeset = changeset,
          uid = uid,
          user = user,
          visible = visible,
          osmType = 0
        )
      case OSMElement.OSMWay(id, nds, tag, version, timestamp, changeset, uid, user, visible) =>
        OSMInternalRow(
          id = id,
          lat = None,
          lon = None,
          nds = nds,
          relations = Seq.empty,
          tag = tag,
          version = version,
          timestamp = timestamp,
          changeset = changeset,
          uid = uid,
          user = user,
          visible = visible,
          osmType = 1
        )
      case OSMElement.OSMRelation(id, relations, tag, version, timestamp, changeset, uid, user, visible) =>
        OSMInternalRow(
          id = id,
          lat = None,
          lon = None,
          nds = Seq.empty,
          relations = relations,
          tag = tag,
          version = version,
          timestamp = timestamp,
          changeset = changeset,
          uid = uid,
          user = user,
          visible = visible,
          osmType = 2
        )
    }
  }
}