package com.ericsun95.spark.osm.pbf.parser

import java.io.InputStream

import com.ericsun95.spark.osm.pbf.model.OSMElement
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob

class PBFFileParser(dis: InputStream) extends Iterator[OSMElement] {
  private val pbfStreamIterator: Iterator[RawBlob] = new PBFStreamParser(dis)
    .withFilter(_.blobtype == PBFFileParser.PRIMITIVE_TYPE)

  private val pbfBlobIterator: Iterator[Blob] = pbfStreamIterator.map(Blob parseFrom _.blob)
  private val pbfElementIterator: Iterator[OSMElement] = pbfBlobIterator.flatMap(new PBFBlobParser(_))

  override def hasNext: Boolean = pbfElementIterator.hasNext

  override def next(): OSMElement = {
    pbfElementIterator.next
  }

}

object PBFFileParser {
  private val HEADER_TYPE = "OSMHeader"
  private val PRIMITIVE_TYPE = "OSMData"
}
