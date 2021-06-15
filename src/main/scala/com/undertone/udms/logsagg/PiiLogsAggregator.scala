package com.undertone.udms.logsagg

import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, LocalDateTime}
import com.undertone.udms.logsagg.dto.{LogType1Schema, SspFullSchema, SspJsonSchema}
import com.undertone.udms.logsagg.utils.{GetNewFiles, LogsTransformationUtil, Parameters}
import com.undertone.udms.utils.{FilePathDateId, HasLogger, IncrementUtilsLambda}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object PiiLogsAggregator extends HasLogger{
  def main(args: Array[String]): Unit = {
    val start: Instant = Instant.now()

    if (args.length == 0) {
      println("Environment parameter is missing [staging | production]")
    }

    val environment: String = args(0)
    logger.info("Loading parameters")
    val parameters = new Parameters(environment)

    val lambdaIncrement: IncrementUtilsLambda = GetNewFiles.getNewFilesWithLambda(parameters.bucketNameSsp, parameters.prefixSsp,
      parameters.regionSsp, parameters.historyDepthSsp, parameters.incrDbUrl, parameters.incrDbUser, parameters.incrDbPassword,
      parameters.jobNameSsp + "-agg", parameters.runIdSsp, parameters.isPartitioned, parameters.fileType,
      parameters.datePartitionPrefixSsp)

    val increment: List[FilePathDateId] = lambdaIncrement.getIncrement
    val sspPaths: List[String] = increment.map(n => "s3://" + parameters.bucketNameSsp + "/" + n.filePath)
    val filesSizeSum: Long = increment.map(n=>n.fileSize).sum

    if (sspPaths.nonEmpty) {
      //spark def
      val conf = new SparkConf()
      conf.set("spark.serializer", parameters.sparkSerializer)
      conf.registerKryoClasses(Array(classOf[LogType1Schema],classOf[SspJsonSchema],classOf[SspFullSchema]))
      conf.set("fs.s3.canned.acl", "BucketOwnerFullControl")
      conf.set("spark.sql.files.ignoreCorruptFiles","true")
      conf.set("spark.files.ignoreCorruptFiles","true")

      val sparkSession: SparkSession = SparkSession
        .builder()
        .enableHiveSupport()
        .appName(getClass.getName)
        .config(conf)
        .getOrCreate()

      logger.info("Getting local date from EMR machine")
      val nowDate = LocalDateTime.now()
      println(nowDate)

      val yearFormatter = DateTimeFormatter.ofPattern("yyyy")
      val monthFormatter = DateTimeFormatter.ofPattern("MM")
      val dayFormatter = DateTimeFormatter.ofPattern("dd")
      val hourFormatter = DateTimeFormatter.ofPattern("HH")
      val minuteFormatter = DateTimeFormatter.ofPattern("mm")

      val year: String = nowDate.format(yearFormatter)
      val month: String = nowDate.format(monthFormatter)
      val day: String = nowDate.format(dayFormatter)
      val hour: String = nowDate.format(hourFormatter)
      val minute: String = nowDate.format(minuteFormatter)

      val outputPathSsp: String = parameters.rampLogsTargetPathJson
        .replace("$rampLogNamePrefix", parameters.rampLogNamePrefixSsp)
        .replace("$Year", year)
        .replace("$Month", month)
        .replace("$Day", day)
        .replace("$Hour", hour)
        .replace("$Minute", minute)

      logger.info("Executing aggregation processing for ssp (deal)")
      logger.info("Total batch size (Bytes): " + filesSizeSum + " On " + sspPaths.size + " files.")
      val sspTotalFileSizeGb: Double = filesSizeSum / 1024.0 / 1024.0 / 1024.0
      logger.info("Total batch size (GB): " + sspTotalFileSizeGb)

      var defaultPartitionSize: Int = 200
      println("maxNormalBatchSizeGb: " + parameters.maxNormalBatchSizeGb)
      println("defaultPartitionSize: " + defaultPartitionSize)
      if (parameters.maxNormalBatchSizeGb<sspTotalFileSizeGb) {
        defaultPartitionSize = (sspTotalFileSizeGb.toDouble / (parameters.maxNormalBatchSizeGb.toDouble / defaultPartitionSize.toDouble)).toInt
      }
      println("defaultPartitionSize after recalculation: " + defaultPartitionSize)
      //general DS from TSV
      val sspDS: Dataset[SspFullSchema] = LogsTransformationUtil.getSspDS(sparkSession, sspPaths, defaultPartitionSize)

      //JSON Agg
      val sspJsonDs: Dataset[SspJsonSchema] = processLogsSSP(sparkSession, sspDS)
      sspJsonDs.coalesce(1).write.option("compression", "gzip").mode(SaveMode.Append).json(outputPathSsp)


      /**
      Amazon S3
      ramplift-datalog-prod-us-east-1
      datalogs/
      pii/
      aws_region=us-east-1/
      dt=20210606/

      for...
          val aggSSPDeals: Dataset[AggSSPDeal] = execute_aggregation(sparkSession,aggSSPExplodedByDeal,device_id)
          if aggSSPDeals
      */


      sparkSession.stop
      //Mark files as done
      lambdaIncrement.setDoneStatus()
      logger.info("END")
      val end: Instant = Instant.now()
      logger.info("Duration: " + Duration.between(start, end))
    } else {
      logger.warn("No files found for ssp log")
    }
  }

  def processLogsSSP(sparkSession: SparkSession,
                     sspDS: Dataset[SspFullSchema]
                    ): Dataset[SspJsonSchema] ={

    sspDS.createOrReplaceTempView("ssp_table")

    import sparkSession.implicits._
    sspDS.sqlContext.sql(
      s"""SELECT deal_id as thirdPartyDealId,
                      SUM(case when bid>0 and result = 'success' then bid else 0 end) AS accumulatingPrice,
                      banner_id as bannerId,
                      Int(COUNT(result)) AS totalRequests,
                      Int(COUNT(case
                      when bid>0 and result = 'success' then 1 end)) AS totalResponses
                      FROM ssp_table
                      where banner_id>0 and nvl(deal_id, '') <> ''
                      GROUP BY thirdPartyDealId, bannerId""").as[SspJsonSchema]
    //SSP Json should always write to one files since the aggregation result will be around 1500 records only.
  }
}
