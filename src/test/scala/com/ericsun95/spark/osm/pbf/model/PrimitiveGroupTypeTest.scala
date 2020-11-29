package com.ericsun95.spark.osm.pbf.model

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PrimitiveGroupTypeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  test("PrimitiveGroupType generated correctly") {

    PrimitiveGroupType.parse("Nodes").get shouldBe PrimitiveGroupType.Nodes
    PrimitiveGroupType.parse("Ways").get shouldBe PrimitiveGroupType.Ways
    PrimitiveGroupType.parse("Relations").get shouldBe PrimitiveGroupType.Relations
    PrimitiveGroupType.parse("Changesets").get shouldBe PrimitiveGroupType.Changesets
    PrimitiveGroupType.parse("DenseNodes").get shouldBe PrimitiveGroupType.DenseNodes

    PrimitiveGroupType.parse("Nodes").get.typeName shouldBe "Nodes"
    PrimitiveGroupType.parse("Ways").get.typeName shouldBe "Ways"
    PrimitiveGroupType.parse("Relations").get.typeName shouldBe "Relations"
    PrimitiveGroupType.parse("Changesets").get.typeName shouldBe "Changesets"
    PrimitiveGroupType.parse("DenseNodes").get.typeName shouldBe "DenseNodes"

  }
}
