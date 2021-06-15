package com.undertone.udms.logsagg

import com.amazonaws.regions.Regions
import com.typesafe.config.Config
import com.undertone.sspdeals.dto.{AggSSPDeal, AggSSPDealsExploded, AggSSPDealsExplodedStep1}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.undertone.udms.utils.GlueService.addPartitions
import com.undertone.udms.utils.{DBService, EnvironmentUtils, HasLogger}

import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter


object PiiLogs extends HasLogger{
  def main(args: Array[String]): Unit = {

    val start: Instant = Instant.now()

    if (args.length < 1) {
      println("Missing parameters: Environment[staging | production]")
    }

    val envIniPath: Map[String, String] = Map(
      "staging" -> "s3://udmp-configurations-nonprod/udmp-ssp-deals-scala/environment.ini",
      "production" -> "s3://udmp-configurations-prod/udmp-ssp-deals-scala/environment.ini"
    )


    val environment: String = args(0)
    val filePath : String = envIniPath(environment)
    val config : Config = EnvironmentUtils.loadConfig(filePath, "us-east-1")


    //calc parameters
    val sspDealsTableNewPath: String = config.getString("sspDealsTableNewPath")
    val srcSchemaName: String = config.getString("srcSchemaName")
    val srcTableName: String = config.getString("srcTableName")
    val requestFunnelTableName: String = srcSchemaName + "." + srcTableName
    val targetSchema: String = config.getString("targetSchema")
    val targetTable: String = config.getString("targetTable")
    val previousJobNames:String = config.getString("previousJobNames")

    val versionId : Long = Instant.now.getEpochSecond
    val historyDepth: Int = config.getInt("historyDepth")


    val currentRunDate: LocalDate = LocalDate.now()
    val runDate : LocalDate =
      if ( args.length>=2)
        LocalDate.parse(args(1), DateTimeFormatter.ofPattern("yyyyMMdd"))
      else currentRunDate

    val dbService: DBService = new DBService(config)

    val dirtyDates:List[String] = dbService.queryDailyVersions(jobName = targetTable,
      previousJobNames = previousJobNames,
      runDate = runDate,
      historyDepth = historyDepth)

    val dirtyDatesString: String =  "'" + dirtyDates.mkString("','") + "'"

    logger.info(s"PROCESSING DATES :  $dirtyDatesString")

    val conf = new SparkConf()

    conf.set("spark.serializer", config.getString("sparkSerializer"))

    conf.registerKryoClasses(Array(classOf[AggSSPDealsExploded],classOf[AggSSPDealsExplodedStep1],classOf[AggSSPDeal]))

    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(getClass.getName)
      .config(conf)
      .getOrCreate()


    val aggSSPExplodedByDeal: Dataset[AggSSPDealsExploded] = execute_explosion(sparkSession,
      requestFunnelTableName,
      dirtyDatesString)

    val aggSSPDeals: Dataset[AggSSPDeal] = execute_aggregation(sparkSession,aggSSPExplodedByDeal)

    aggSSPDeals
      .write
      .partitionBy("dt")
      .mode(SaveMode.Overwrite)
      .parquet(sspDealsTableNewPath)


    for ( dt <- dirtyDates) {

      logger.info(s"ADDING PARTITION :  $dt")

      addPartitions(sspDealsTableNewPath,
        Regions.US_EAST_1,
        targetSchema,
        targetTable,
        Seq("dt"),
        Seq(dt))

      dbService.addDailyVersion(jobName = targetTable,
        dateId = dt.toInt,
        versionId = versionId)

    }

    sparkSession.stop

    val end: Instant = Instant.now()

    dbService.insertProcessingMetrics(jobName = targetTable,
      processedDates = dirtyDatesString,
      versionId = versionId,
      startTimestamp = start,
      finishTimestamp = end)

  }

  def execute_aggregation(sparkSession: SparkSession,
                        aggSSPDealsExploded:  Dataset[AggSSPDealsExploded]) : Dataset[AggSSPDeal] = {

    import sparkSession.implicits._

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.maxRetries", "20")

    aggSSPDealsExploded.createOrReplaceTempView("aggSSPDeals")

    sparkSession.sqlContext.sql(
      s"""
          select device_id
          from dl_udms_work.dl_device
          where device_id = $device_id
          """).as[AggSSPDeal]
  }

  def execute_explosion(sparkSession: SparkSession,
                          requestFunnelTableName: String,
                          dirtyDates: String) : Dataset[AggSSPDealsExploded] = {

    import sparkSession.implicits._

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3.maxRetries", "20")

    val explodedDS:Dataset[AggSSPDealsExplodedStep1] =

      sparkSession.sqlContext.sql(
        s"""
          select   dt,
                   date_trunc('day',request_po_date) as request_po_date,
                   date_trunc('day',request_utc_date) as request_utc_date,
                   case when geo_country in ('uk','us','mx','ca')
                        then geo_country
                        else 'N/A'
                   end as geo_country,
                   publisher_id,
                   explode(request_deals) as deal_id,
                   selected_deal,
                   device_type,
                   case when response_programmatic_deal_type = 'Direct'
                        then 0
                        else 1
                   end as is_programmatic,
                   media_supply_source_id as  supply_source_id,
                   publisher_ut_placement_group_id as ut_placement_group_id,
                   response_selected_ad_unit_id as ad_unit_id,
                   delivery_requests,
                   delivery_impressions,
                   delivery_clicks,
                   delivery_passbacks,
                   publisher_requests,
                   creative_responses,
                   creative_wins,
                   creative_cost,
                   COALESCE(ias_inview1s_impressions,0) as ias_inview1s_impressions,
                   ias_inview5s_impressions,
                   ias_inview15s_impressions,
                   ias_fullyinview0s_impressions,
                   ias_fullyinview1s_impressions,
                   ias_fullyinview5s_impressions,
                   ias_fullyinview15s_impressions,
                   ias_video_vtc_impressions,
                   ias_inview10s_impressions,
                   ias_inview30s_impressions,
                   ias_measured_impression,
                   hb_win_cost,
                   request_deal_floor_price
          from $requestFunnelTableName
          where  dt in ($dirtyDates) and not array_contains(request_deals,'N/A')
        """).as[AggSSPDealsExplodedStep1]

    explodedDS.createOrReplaceTempView("explodedBySSPDeal")

    sparkSession.sqlContext.sql(
      s"""
          select dt,
                 request_po_date,
                 request_utc_date,
                 geo_country,
                 publisher_id,
                 element_at(split(deal_id,'__'),1) as deal_id,
                 selected_deal,
                 device_type,
                 is_programmatic,
                 supply_source_id,
                 ut_placement_group_id,
                 ad_unit_id,
                 delivery_requests,
                 delivery_impressions,
                 delivery_clicks,
                 delivery_passbacks,
                 publisher_requests,
                 creative_responses,
                 creative_wins,
                 creative_cost,
                 ias_inview1s_impressions,
                 ias_inview5s_impressions,
                 ias_inview15s_impressions,
                 ias_fullyinview0s_impressions,
                 ias_fullyinview1s_impressions,
                 ias_fullyinview5s_impressions,
                 ias_fullyinview15s_impressions,
                 ias_video_vtc_impressions,
                 ias_inview10s_impressions,
                 ias_inview30s_impressions,
                 ias_measured_impression,
                 hb_win_cost,
                 COALESCE(
                 case when cast(element_at(split(deal_id,'__'),2) as double) = 0
                      then request_deal_floor_price
                      else cast(element_at(split(deal_id,'__'),2) as double)
                 end, -1) as request_deal_floor_price
          from explodedBySSPDeal
        """).as[AggSSPDealsExploded]
  }
}
