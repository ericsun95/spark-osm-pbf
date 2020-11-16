package com.ericsun95.spark.osm.pbf.parser

import java.util.logging.Logger
import java.util.zip.{DataFormatException, Inflater}

import com.ericsun95.spark.osm.pbf.model.OSMElement
import com.ericsun95.spark.osm.pbf.model.OSMElement.{OSMNode, OSMRelation, OSMWay}
import org.openstreetmap.osmosis.osmbinary.fileformat.Blob
import org.openstreetmap.osmosis.osmbinary.osmformat._

class PBFBlobParser(blob: Blob) extends Iterator[OSMElement] {

  private lazy val logger = Logger.getLogger(this.getClass.getName)
  private val primitiveBlock: PrimitiveBlock = PrimitiveBlock parseFrom blobDecompressor(blob)
  private var groupIdCursor: Int = 0
  private var elementIdCursor: Int = 0
  private var denseNodesSeq: Option[Seq[OSMNode]] = None

  override def hasNext: Boolean = primitiveBlock.primitivegroup.size != groupIdCursor

  def nextPrimitiveGroup(): Unit = {
    groupIdCursor += 1
    elementIdCursor = 0
  }

  def populateNode(currentPrimitiveGroup: PrimitiveGroup): OSMNode = {
//    logger.info(s"Populate osm node ${elementIdCursor} in nodes group")
    val osmosisNode: Node = currentPrimitiveGroup.nodes(elementIdCursor)
    elementIdCursor += 1
    if(elementIdCursor >= currentPrimitiveGroup.nodes.size) {
      nextPrimitiveGroup()
    }
    OSMNode(
      osmosisNode,
      primitiveBlock.stringtable,
      primitiveBlock.latOffset,
      primitiveBlock.lonOffset,
      primitiveBlock.granularity,
      primitiveBlock.dateGranularity
    )
  }


  def populateWay(currentPrimitiveGroup: PrimitiveGroup): OSMWay = {
//    logger.info(s"Populate osm way ${elementIdCursor} in ways group")
    val osmosisWay: Way = currentPrimitiveGroup.ways(elementIdCursor)
    elementIdCursor += 1
    if(elementIdCursor >= currentPrimitiveGroup.ways.size)
      nextPrimitiveGroup()
    OSMWay(
      osmosisWay,
      primitiveBlock.stringtable,
      primitiveBlock.dateGranularity
    )
  }


  def populateRelation(currentPrimitiveGroup: PrimitiveGroup): OSMRelation = {
//    logger.info(s"Populate osm relation ${elementIdCursor} in relations group")
    val osmosisRelation: Relation = currentPrimitiveGroup.relations(elementIdCursor)
    elementIdCursor += 1
    if(elementIdCursor >= currentPrimitiveGroup.relations.size)
      nextPrimitiveGroup()
    OSMRelation(
      osmosisRelation,
      primitiveBlock.stringtable,
      primitiveBlock.dateGranularity
    )
  }

  def populateDenseNode(currentPrimitiveGroup: PrimitiveGroup): OSMNode = {
//    logger.info(s"Populate osm dense node ${elementIdCursor} in dense node group")
    val osmosisDenseNode: DenseNodes = currentPrimitiveGroup.dense.get
    if(elementIdCursor == 0 && denseNodesSeq.isEmpty) {
      denseNodesSeq = Some(OSMNode(
        osmosisDenseNode,
        primitiveBlock.stringtable,
        primitiveBlock.latOffset,
        primitiveBlock.lonOffset,
        primitiveBlock.granularity,
        primitiveBlock.dateGranularity
      ))
    }
    val result = denseNodesSeq.get(elementIdCursor)
    elementIdCursor += 1
    if(denseNodesSeq.nonEmpty && elementIdCursor >= denseNodesSeq.get.size) {
      denseNodesSeq = None
      nextPrimitiveGroup()
    }
    result
  }

  override def next(): OSMElement = {
    extractPrimitiveGroup(currentPrimitiveGroup = primitiveBlock.primitivegroup(groupIdCursor))
  }

  //TODO: Add filter here to speed up filtering
  def extractPrimitiveGroup(currentPrimitiveGroup: PrimitiveGroup): OSMElement = {
    currentPrimitiveGroup match {
      case _ if currentPrimitiveGroup.nodes.nonEmpty => populateNode(currentPrimitiveGroup)
      case _ if currentPrimitiveGroup.ways.nonEmpty => populateWay(currentPrimitiveGroup)
      case _ if currentPrimitiveGroup.relations.nonEmpty => populateRelation(currentPrimitiveGroup)
      case _ if currentPrimitiveGroup.changesets.nonEmpty => throw new Exception ("No support on changeset")
      case _ if currentPrimitiveGroup.dense.isDefined => populateDenseNode(currentPrimitiveGroup)
    }
  }


  def blobDecompressor(blob: Blob): Array[Byte] = blob match {
    case _ if blob.raw.isDefined => blob.raw.get.toByteArray
    case _ if blob.zlibData.isDefined =>
      val inflater = new Inflater()
      inflater.setInput(blob.zlibData.get.toByteArray)
      val blobData = new Array[Byte](blob.rawSize.get)
      try {
        inflater.inflate(blobData)
      } catch {
        case e : DataFormatException => throw new RuntimeException("Unable to decompress PBF blob.", e)
      }
      if (inflater.finished()) {
        logger.info("Inflater finished for this blob")
        blobData
      } else {
        throw new RuntimeException("PBF blob uses unsupported compression, only raw or zlib may be used.")
      }
  }

}

object PBFBlobParser {}
