package com.mert.main

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

object ClickDataMain {

  private var inputDataPath: String = null
  private var outputPath: String = null

  def main(args: Array[String]): Unit = {

    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val clickData = getClickDataSet(env)

    // Unique Product View counts by ProductId
    val uniqueProductViewCountsByProductId = getUniqueProductViewCountsByProductId(clickData);

    writeToTxtFileCsvStyle(uniqueProductViewCountsByProductId, "uniqueProductViewCountsByProductId.txt")

    // Unique Event counts
    val uniqueEventCounts = getUniqueEventCounts(clickData)
    writeToTxtFileCsvStyle(uniqueEventCounts, "uniqueEventCounts.txt")


    // All events of #UserId : 47
    val allEventsOfUserId47 = getAllEventsOfUserId47(clickData)
    writeToTxtFileCsvStyle(allEventsOfUserId47, "allEventsOfUserId47.txt")

    // Product Views of #UserId : 47
    val productViewsOfUserId47 = getProductViewsOfUserId47(clickData)

    writeToTxtFile(productViewsOfUserId47, "productViewsOfUserId47.txt")

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

  def writeToTxtFile(dataSet: DataSet[String], fileName: String) = {
    val filePath = outputPath + fileName
    dataSet.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(1)

  }

  def writeToTxtFileCsvStyle(dataSet: AggregateDataSet[(String, Int)], fileName: String) = {

    val filePath = outputPath + fileName
    dataSet.writeAsCsv(filePath, fieldDelimiter = "|", writeMode = WriteMode.OVERWRITE).setParallelism(1)

  }

  def getUniqueProductViewCountsByProductId(clickStreamingData: DataSet[ClickDataModel]) = {
    val uniqueProductViewCountsByProductId = clickStreamingData.filter(x => x.eventName == "view").map(x => (x.productId, 1)).groupBy(0).sum(1)
    uniqueProductViewCountsByProductId
  }

  def getUniqueEventCounts(clickStreamingData: DataSet[ClickDataModel]): AggregateDataSet[(String, Int)] = {

    val uniqueEventCounts: AggregateDataSet[(String, Int)] = clickStreamingData.map(x => (x.eventName, 1)).groupBy(0).sum(1)
    uniqueEventCounts
  }

  def getAllEventsOfUserId47(clickStreamingData: DataSet[ClickDataModel]) = {

    val allEventsOfUserId47 = clickStreamingData.filter(x => x.userId == "47").map(x => (x.eventName, 1)).groupBy(0).sum(1)

    allEventsOfUserId47
  }

  def getProductViewsOfUserId47(clickStreamingData: DataSet[ClickDataModel]) = {

    val productViewsOfUserId47 = clickStreamingData.filter(x => x.userId == "47" && x.eventName == "view").map(x => x.productId)
    productViewsOfUserId47
  }
}
