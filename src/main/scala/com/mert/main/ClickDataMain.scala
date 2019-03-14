package com.mert.main

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row

object ClickDataMain {

  private var inputDataPath: String = null
  private var outputPath: String = null
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)


  def main(args: Array[String]): Unit = {

    if (!parseParameters(args)) {
      return
    }

    val clickData = getClickDataSet(env)
    tEnv.registerDataSet("click_data",clickData)
    // Unique Product View counts by ProductId
    val uniqueProductViewCountsByProductId = getUniqueProductViewCountsByProductId()

    writeToTxtFileCsvStyle(uniqueProductViewCountsByProductId, "uniqueProductViewCountsByProductId.txt")

    // Unique Event counts
    val uniqueEventCounts = getUniqueEventCounts
    writeToTxtFileCsvStyle(uniqueEventCounts, "uniqueEventCounts.txt")

    // All events of #UserId : 47
    val allEventsOfUserId47 = getAllEventsOfUserId47
    writeToTxtFileCsvStyle(allEventsOfUserId47, "allEventsOfUserId47.txt")
    // Product Views of #UserId : 47
    val productViewsOfUserId47 = getProductViewsOfUserId47

    writeToTxtFile(productViewsOfUserId47, "productViewsOfUserId47.txt")

    // Top 5 Users who fulfilled all the events (view,add,remove,click)
    val top5UsersWhofullfilledAllEvents = getTop5UsersWhofullfilledAllEvents()

    writeToTxtFile(top5UsersWhofullfilledAllEvents, "top5UsersWhofullfilledAllEvents.txt")

    env.execute("ClickStream Data")
  }

  def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputDataPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.err.println(
        " Usage: ClickDataProject <clickdata-csv path> <clickdata output path> ")
      false
    }
  }

  def getClickDataSet(env: ExecutionEnvironment) = {
    env.readCsvFile[ClickDataModel](
      inputDataPath,
      ignoreFirstLine = true,
      fieldDelimiter = "|")
  }

  def writeToTxtFile(table: Table, fileName: String) = {
    val filePath = outputPath + fileName
    table.toDataSet[Row].writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(1)

  }

  def writeToTxtFileCsvStyle(table: Table, fileName: String) = {

    val filePath = outputPath + fileName
    table.toDataSet[(String,Long)].writeAsCsv(filePath, fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

  }

  def getUniqueProductViewCountsByProductId() = {
    val uniqueProductViewCountsByProductId = tEnv.sqlQuery("select productId,count(1) from click_data where eventName='view' group by productId")
    uniqueProductViewCountsByProductId
  }

  def getUniqueEventCounts() = {

    val uniqueEventCounts = tEnv.sqlQuery("select eventName,count(1) from click_data group by eventName")
    uniqueEventCounts
  }

  def getAllEventsOfUserId47() = {
    val allEventsOfUserId47 = tEnv.sqlQuery("select eventName,count(1) from click_data where userId='47' group by eventName")

    allEventsOfUserId47
  }

  def getProductViewsOfUserId47() = {

    val productViewsOfUserId47 = tEnv.sqlQuery("select distinct productId from click_data where userId='47' ")
    productViewsOfUserId47
  }

  def getTop5UsersWhofullfilledAllEvents() = {

    val Top5UsersWhofullfilledAllEvents = tEnv.sqlQuery("select userId from click_data group by userId having count(distinct eventName)>3 order by 1 desc limit 5")
    Top5UsersWhofullfilledAllEvents
  }
}
