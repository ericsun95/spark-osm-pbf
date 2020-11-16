package com.ericsun95.spark.osm.pbf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob

class OsmPbfInputPartition(blob: Blob) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new OsmPbfInputPartitionReader(blob)
}
