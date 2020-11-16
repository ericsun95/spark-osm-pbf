package com.ericsun95.spark.osm.pbf

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DefaultSource extends DataSourceV2 with ReadSupport with DataSourceRegister {

  //TODO: Remove it?
  override def shortName(): String = "osm.pbf"

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new OsmPbfDataSourceReader(options.paths())
  }

}
