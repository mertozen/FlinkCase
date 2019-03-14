package com.mert.test

import org.apache.flink.api.scala._
import com.mert.main.{ClickDataMain, ClickDataModel}
import org.apache.commons.compress.utils.Lists
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.Test
import org.junit.Assert._


class ClickDataTest  {

  @Test
  def testGetUniqueProductViewCountsByProductId(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val sampleClickData = env.fromElements(ClickDataModel("1535816823","496","view","13" ),ClickDataModel("1536392928","496","add","69"))

    assertEquals(Seq(("496",1)), ClickDataMain.getUniqueProductViewCountsByProductId(sampleClickData).collect())
  }

  @Test
  def testGetAllEventsOfUserId47(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val sampleClickData = env.fromElements(ClickDataModel("1535816823","567","add","47" ),ClickDataModel("1535816823","496","view","47" ),ClickDataModel("1536392928","496","add","69"))

    assertEquals(Seq("add","view"), ClickDataMain.getAllEventsOfUserId47(sampleClickData).collect())
  }

  @Test
  def testGetUniqueEventCounts(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val sampleClickData = env.fromElements(ClickDataModel("1535816823","496","add","13" ),ClickDataModel("1536392928","496","add","69"))
    ClickDataMain.getUniqueEventCounts(sampleClickData)

    assertEquals(Seq(("add",2)), ClickDataMain.getUniqueEventCounts(sampleClickData).collect())
  }

  @Test
  def testGetProductViewsOfUserId47(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val sampleClickData = env.fromElements(ClickDataModel("1535816823","496","view","47" ),ClickDataModel("1536392928","496","add","69"))

    assertEquals(Seq("47"), ClickDataMain.getProductViewsOfUserId47(sampleClickData).collect())
  }

}
