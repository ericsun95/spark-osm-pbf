package com.ericsun95.spark.osm.pbf.parser

import java.io.{DataInputStream, EOFException, IOException, InputStream}
import java.util.logging.Logger

import org.openstreetmap.osmosis.osmbinary.fileformat.BlobHeader

class PBFStreamParser(dis: InputStream) extends Iterator[RawBlob] {

  private val logger = Logger.getLogger(this.getClass.getName)
  private val dataInputStream: DataInputStream = new DataInputStream(dis)
  private var dataBlockCount: Int = 0
  private var endOfStream: Boolean = false
  private var headerLength: Option[Int] = None
  private var nextBlob: Option[RawBlob] = None

  private def readHeader(headLength: Int): BlobHeader = {
    val headerBuffer = new Array[Byte](headLength)
    dataInputStream.readFully(headerBuffer)
    BlobHeader parseFrom headerBuffer
  }

  private def readBlob(blobHeader: BlobHeader): Array[Byte] = {
    val blob = new Array[Byte](blobHeader.datasize)
    // Currently, this step is the bottleneck when reading gb data with limited memory
    dataInputStream.readFully(blob)
    blob
  }

  override def next(): RawBlob = {
    val result = nextBlob
    nextBlob = None
    result.get
  }

  override def hasNext: Boolean = {
    //If it's the beginning, try getNext, if get it hasNext else None
    if(nextBlob.isEmpty && !endOfStream) {
      getNextBlob()
    }
    nextBlob.nonEmpty
  }


  def getHeaderLength(): Option[Int] = {
    try {
      Some(dataInputStream.readInt())
    } catch {
      case _: EOFException =>
        logger.info("Reach the end of the data input stream")
        endOfStream = true
        None
    }
  }

  def getNextBlob(): Unit = {
    try {
      headerLength = getHeaderLength()
      dataBlockCount += 1
      nextBlob = headerLength.map(hl => {
//        logger.info(s"Reading header for blob ${dataBlockCount}, header length ${hl}")
        val blobHeader: BlobHeader = readHeader(hl)
//        logger.info(s"Processing blob of type ${blobHeader.`type`}.")
        val blobData: Array[Byte] = readBlob(blobHeader)
        RawBlob(blobHeader.`type`, blobData)
      })
    } catch {
      case e: IOException =>throw new RuntimeException ("Unable to get next blob from PBF stream.", e)
    }
  }

}


case class RawBlob(blobtype: String, blob: Array[Byte])
