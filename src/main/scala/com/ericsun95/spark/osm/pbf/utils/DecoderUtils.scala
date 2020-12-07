package com.ericsun95.spark.osm.pbf.utils

import org.openstreetmap.osmosis.osmbinary.osmformat.{Info, StringTable}

trait DecoderUtils {

  val COORDINATE_SCALING_FACTOR: Double = 0.000000001
  val DEFAULT_LAT_OFFSET: Long = 0L
  val DEFAULT_LON_OFFSET: Long = 0L
  val DEFAULT_GRANULARITY: Int = 100
  val DEFAULT_DATE_GRANULARITY: Int = 1000
  val DEFAULT_CHARSET: String = "UTF-8"

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

  def getCumulativeSum[A](xs: Seq[A])(implicit num: Numeric[A]): Seq[A] = {
    xs.tail.scanLeft(xs.head)(num.plus)
  }

}
