package com.ericsun95.spark.osm.pbf.model

import java.io.Serializable

import org.openstreetmap.osmosis.osmbinary.osmformat.PrimitiveGroup

sealed trait PrimitiveGroupType extends Product with Serializable {
  def typeName: String
}

object PrimitiveGroupType {

  case object Nodes extends PrimitiveGroupType {
     override def typeName = "Nodes"
  }
  case object Ways extends PrimitiveGroupType {
    override def typeName = "Ways"
  }
  case object Relations extends PrimitiveGroupType {
    override def typeName = "Relations"
  }
  case object Changesets extends PrimitiveGroupType {
    override def typeName = "Changesets"
  }
  case object DenseNodes extends PrimitiveGroupType {
    override def typeName = "DenseNodes"
  }

  /**
   * Get PrimitiveBlockType based on its name
   * @param name
   * @return
   */
  def parse(name: String): Option[PrimitiveGroupType] = {
    Vector(Nodes, Ways, Relations, Changesets, DenseNodes).find(_.toString == name)
  }

  /**
   * Get PrimitiveBlockType from PrimitiveGroup
   * @param currentPrimitiveGroup
   * @return
   */
  def apply(currentPrimitiveGroup: PrimitiveGroup): PrimitiveGroupType = currentPrimitiveGroup match {
    case _ if currentPrimitiveGroup.nodes.nonEmpty => Nodes
    case _ if currentPrimitiveGroup.ways.nonEmpty => Ways
    case _ if currentPrimitiveGroup.relations.nonEmpty => Relations
    case _ if currentPrimitiveGroup.changesets.nonEmpty => Changesets
    case _ if currentPrimitiveGroup.dense.isDefined => DenseNodes
  }

}
