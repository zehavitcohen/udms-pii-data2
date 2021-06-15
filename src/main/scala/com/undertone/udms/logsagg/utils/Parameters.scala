package com.undertone.udms.logsagg.utils

import java.util.UUID.randomUUID

import com.typesafe.config.Config
import com.undertone.udmp.utils.{EnvironmentUtils, HasLogger}

class Parameters(environment: String) extends HasLogger{
  val envIniPath: Map[String, String] = Map(
    "staging" -> "s3://udmp-configurations-nonprod/udmp-logsaggregator-scala/environment.ini",
    "production" -> "s3://udmp-configurations-prod/udmp-logsaggregator-scala/environment.ini"
  )

  val filePath : String = envIniPath(environment)
  logger.info(filePath)
  val config : Config = EnvironmentUtils.loadConfig(filePath, "us-east-1")

  //get Spark params
  val sparkSerializer: String = config.getString("sparkSerializer")
  logger.info(sparkSerializer)

  //get general increment params
  val incrDbUrl: String = config.getString("filesIncrement.incrDbUrl")
  logger.info(incrDbUrl)
  val incrDbUser: String = config.getString("filesIncrement.incrDbUser")
  logger.info(incrDbUser)
  val incrDbPassword: String = config.getString("filesIncrement.incrDbPassword")
  logger.info(incrDbPassword)
  val isPartitioned: Boolean = config.getBoolean("filesIncrement.isPartitioned")
  logger.info(isPartitioned.toString)
  val fileType: String = config.getString("filesIncrement.fileType")
  logger.info(fileType)

  //get params per log type
  //requests
  val bucketNameReq: String = config.getString("filesIncrement.requests.bucketName")
  logger.info(bucketNameReq)
  val prefixReq: String = config.getString("filesIncrement.requests.prefix")
  logger.info(prefixReq)
  val regionReq: String = config.getString("filesIncrement.requests.region")
  logger.info(regionReq)
  val historyDepthReq: Int = config.getInt("filesIncrement.requests.historyDepth")
  logger.info(historyDepthReq.toString)
  val jobNameReq : String = config.getString("filesIncrement.requests.jobName")
  logger.info(jobNameReq)
  val runIdReq: String = randomUUID().toString
  logger.info(runIdReq)
  val rampLogNamePrefixReq: String = config.getString("filesIncrement.requests.rampLogNamePrefix")
  logger.info(rampLogNamePrefixReq)

  //impressions
  val bucketNameImp: String = config.getString("filesIncrement.impressions.bucketName")
  logger.info(bucketNameImp)
  val regionImp: String = config.getString("filesIncrement.impressions.region")
  logger.info(regionImp)
  val historyDepthImp: Int = config.getInt("filesIncrement.impressions.historyDepth")
  logger.info(historyDepthImp.toString)
  val runIdImp: String = randomUUID().toString
  logger.info(runIdImp)
  val rampLogNamePrefixImp: String = config.getString("filesIncrement.impressions.rampLogNamePrefix")
  logger.info(rampLogNamePrefixImp)
  
  val datePartitionPrefixImpressions: String = "dt"
  logger.info(datePartitionPrefixImpressions)  


//imp.east
  val prefixImpEast: String = config.getString("filesIncrement.impressions.east.prefix")
  logger.info(prefixImpEast)
  val jobNameImpEast : String = config.getString("filesIncrement.impressions.east.jobName")
  logger.info(jobNameImpEast)
  val runIdImpEast: String = randomUUID().toString
  logger.info(runIdImpEast)

  //imp.west
  val prefixImpWest: String = config.getString("filesIncrement.impressions.west.prefix")
  logger.info(prefixImpWest)
  val jobNameImpWest : String = config.getString("filesIncrement.impressions.west.jobName")
  logger.info(jobNameImpWest)
  val runIdImpWest: String = randomUUID().toString
  logger.info(runIdImpWest)

  //clicks general
  val bucketNameClk: String = config.getString("filesIncrement.clicks.bucketName")
  logger.info(bucketNameClk)
  val regionClk: String = config.getString("filesIncrement.clicks.region")
  logger.info(regionClk)
  val historyDepthClk: Int = config.getInt("filesIncrement.clicks.historyDepth")
  logger.info(historyDepthClk.toString)
  val rampLogNamePrefixClk: String = config.getString("filesIncrement.clicks.rampLogNamePrefix")
  logger.info(rampLogNamePrefixClk)
  val datePartitionPrefixClicks: String = config.getString("filesIncrement.clicks.datePartitionPrefix")
  logger.info(datePartitionPrefixClicks)

  //click.east
  val prefixClkEast: String = config.getString("filesIncrement.clicks.east.prefix")
  logger.info(prefixClkEast)
  val jobNameClkEast : String = config.getString("filesIncrement.clicks.east.jobName")
  logger.info(jobNameClkEast)
  val runIdClkEast: String = randomUUID().toString
  logger.info(runIdClkEast)

  //click.west
  val prefixClkWest: String = config.getString("filesIncrement.clicks.west.prefix")
  logger.info(prefixClkWest)
  val jobNameClkWest : String = config.getString("filesIncrement.clicks.west.jobName")
  logger.info(jobNameClkWest)
  val runIdClkWest: String = randomUUID().toString
  logger.info(runIdClkWest)

  //events general
  val bucketNameEvt: String = config.getString("filesIncrement.events.bucketName")
  logger.info(bucketNameEvt)
  val regionEvt: String = config.getString("filesIncrement.events.region")
  logger.info(regionEvt)
  val historyDepthEvt: Int = config.getInt("filesIncrement.events.historyDepth")
  logger.info(historyDepthEvt.toString)
  val rampLogNamePrefixEvt: String = config.getString("filesIncrement.events.rampLogNamePrefix")
  logger.info(rampLogNamePrefixEvt)
  val datePartitionPrefixEvents: String = config.getString("filesIncrement.events.datePartitionPrefix")
  logger.info(datePartitionPrefixEvents)

  //events.east
  val prefixEvtEast: String = config.getString("filesIncrement.events.east.prefix")
  logger.info(prefixEvtEast)
  val jobNameEvtEast : String = config.getString("filesIncrement.events.east.jobName")
  logger.info(jobNameEvtEast)
  val runIdEvtEast: String = randomUUID().toString
  logger.info(runIdEvtEast)

  //events.west
  val prefixEvtWest: String = config.getString("filesIncrement.events.west.prefix")
  logger.info(prefixEvtWest)
  val jobNameEvtWest : String = config.getString("filesIncrement.events.west.jobName")
  logger.info(jobNameEvtWest)
  val runIdEvtWest: String = randomUUID().toString
  logger.info(runIdEvtWest)

  //ssp general
  val prefixSsp: String = config.getString("filesIncrement.ssp.prefix")
  logger.info(prefixSsp)
  val bucketNameSsp: String = config.getString("filesIncrement.ssp.bucketName")
  logger.info(bucketNameSsp)
  val regionSsp: String = config.getString("filesIncrement.ssp.region")
  logger.info(regionSsp)
  val historyDepthSsp: Int = config.getInt("filesIncrement.ssp.historyDepth")
  logger.info(historyDepthSsp.toString)
  val datePartitionPrefixSsp: String = config.getString("filesIncrement.ssp.datePartitionPrefix")
  logger.info(datePartitionPrefixSsp)
  val rampLogNamePrefixSsp: String = config.getString("filesIncrement.ssp.rampLogNamePrefix")
  logger.info(rampLogNamePrefixSsp)
  val jobNameSsp: String = config.getString("filesIncrement.ssp.jobName")
  logger.info(jobNameSsp)
  val runIdSsp: String = randomUUID().toString
  logger.info(runIdSsp)

  //get general job params
  var rampLogsTargetPathJson: String = config.getString("rampLogsTargetPathJson")
  val sspParquetTablePath: String = config.getString("sspParquetTablePath")
  val maxNormalBatchSizeGb: Int = config.getInt("maxNormalBatchSizeGb")
}
