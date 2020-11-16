package com.ericsun95.spark.osm.pbf

import com.ericsun95.spark.osm.pbf.model.OSMInternalRow
import com.ericsun95.spark.osm.pbf.parser.PBFBlobParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.{Encoder, Encoders}
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob

class OsmPbfInputPartitionReader(blob: Blob) extends InputPartitionReader[InternalRow] {

  private val blobParser = new PBFBlobParser(blob)

  val osmInternalRowEncoder: Encoder[OSMInternalRow] = Encoders.product[OSMInternalRow]
  val osmInternalRowExprEncoder: ExpressionEncoder[OSMInternalRow] =
    osmInternalRowEncoder.asInstanceOf[ExpressionEncoder[OSMInternalRow]]

  var currRow: InternalRow = _

  override def next(): Boolean = {
    if(blobParser.hasNext) {
      val tempRow = OSMInternalRow(blobParser.next())
      currRow = osmInternalRowExprEncoder.toRow(tempRow)
      return true
    }
    blobParser.hasNext
  }

  override def get(): InternalRow = currRow

  override def close(): Unit = {}
}

object OsmPbfInputPartitionReader {}
