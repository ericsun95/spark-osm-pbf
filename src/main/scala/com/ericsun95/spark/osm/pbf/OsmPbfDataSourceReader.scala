package com.ericsun95.spark.osm.pbf

import java.io.{FileInputStream, InputStream}
import java.util

import com.ericsun95.spark.osm.pbf.model.OSMInternalRow
import com.ericsun95.spark.osm.pbf.parser.{PBFFileParser, PBFStreamParser, RawBlob}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob
import scala.collection.JavaConverters._

class OsmPbfDataSourceReader(paths: Array[String]) extends DataSourceReader {

  //TODO: Support paths and more options here
  private val path: String = paths.head
  @transient
  private val pbfInputStream: InputStream = new FileInputStream(path)
  @transient
  private val pbfStreamIterator: Iterator[RawBlob] = new PBFStreamParser(pbfInputStream)
    .withFilter(_.blobtype == PBFFileParser.PRIMITIVE_TYPE)
  @transient
  private val pbfBlobIterator: Iterator[Blob] = pbfStreamIterator.map(Blob parseFrom _.blob)

  override def readSchema(): StructType = OsmPbfDataSourceReader.DEFAULT_SCHEMA

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    pbfBlobIterator.toList.map(blob => {
      new OsmPbfInputPartition(blob).asInstanceOf[InputPartition[InternalRow]]
    }).asJava
  }

}

object OsmPbfDataSourceReader {
  private val DEFAULT_SCHEMA = ScalaReflection.schemaFor[OSMInternalRow].dataType.asInstanceOf[StructType]
}
