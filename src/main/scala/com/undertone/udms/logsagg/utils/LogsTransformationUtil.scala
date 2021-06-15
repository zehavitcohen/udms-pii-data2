package com.undertone.udms.logsagg.utils

import com.undertone.udmp.logsagg.dto.{LogType1Schema, LogEventTypeSchema, SspFullSchema}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object LogsTransformationUtil {

  def processLogsType1(sparkSession: SparkSession,
                       strTimestampLoc: Int,
                       zoneIdLoc: Int,
                       bannerIdLoc: Int,
                       input: List[String],
                       outputS3Path: String,
                       saveMode: SaveMode
                      ): Unit ={

    val inputListBuffer :ListBuffer[String] = input.to[ListBuffer]
    val logsDF: DataFrame = sparkSession.read.option("delimiter","\t").csv(inputListBuffer: _*)

    logsDF.createOrReplaceTempView("log_table")

    import sparkSession.implicits._

    val logsType1Schema: Dataset[LogType1Schema] = logsDF.sqlContext.sql(
      s"""select
                          to_unix_timestamp(to_timestamp(substring(strTimestamp,1,10),'yyyyMMddHH'))*1000 as intervalStart,
                          zoneId,
                          bannerId,
                          count(1) as count
                          from (select trim(_c$strTimestampLoc) as strTimestamp, Int(regexp_extract(trim(_c$zoneIdLoc),'(\\\\d+)', 1)) as zoneId, Int(regexp_extract(trim(_c$bannerIdLoc),'(\\\\d+)', 1)) as bannerId from log_table
                          where Int(trim(_c$zoneIdLoc)) > 0 and Int(trim(_c$bannerIdLoc)) > 0)
                          group by 1,2,3""").as[LogType1Schema]
    logsType1Schema.repartitionByRange(logsType1Schema("intervalStart")).write.option("compression", "gzip").mode(saveMode).json(outputS3Path)
  }

  def processLogsEventType(sparkSession: SparkSession,
                       strTimestampLoc: Int,
                       zoneIdLoc: Int,
                       bannerIdLoc: Int,
                       eventTypeLoc: Int,
                       input: List[String],
                       outputS3Path: String,
                       saveMode: SaveMode
                      ): Unit ={

    val inputListBuffer :ListBuffer[String] = input.to[ListBuffer]
    val logsDF: DataFrame = sparkSession.read.option("delimiter","\t").csv(inputListBuffer: _*)

    logsDF.createOrReplaceTempView("log_table")

    import sparkSession.implicits._

    val logEventTypeSchema: Dataset[LogEventTypeSchema] = logsDF.sqlContext.sql(
      s"""select
                          to_unix_timestamp(to_timestamp(substring(strTimestamp,1,10),'yyyyMMddHH'))*1000 as intervalStart,
                          zoneId,
                          bannerId,
                          eventType,
                          count(1) as count
                          from (select trim(_c$strTimestampLoc) as strTimestamp, Int(regexp_extract(trim(_c$zoneIdLoc),'(\\\\d+)', 1)) as zoneId,
                          Int(regexp_extract(trim(_c$bannerIdLoc),'(\\\\d+)', 1)) as bannerId,
                          trim(_c$eventTypeLoc) as eventType
                          from log_table
                          where Int(trim(_c$zoneIdLoc)) > 0 and Int(trim(_c$bannerIdLoc)) > 0 and trim(_c$eventTypeLoc) = 'complete')
                          group by 1,2,3,4""").as[LogEventTypeSchema]
    logEventTypeSchema.repartitionByRange(logEventTypeSchema("intervalStart")).write.option("compression", "gzip").mode(saveMode).json(outputS3Path)
  }
  def getSspDS(sparkSession: SparkSession, input: List[String], defaultPartitionSize:Int): Dataset[SspFullSchema] = {
    val inputListBuffer :ListBuffer[String] = input.to[ListBuffer]
    val sspDF: DataFrame = sparkSession.read.option("delimiter","\t").csv(inputListBuffer: _*)
    val cleanNullsDF: DataFrame = sspDF.na.replace(sspDF.columns.toSeq, Map("\\N" -> ""))
    cleanNullsDF.createOrReplaceTempView("ssp_df")
    sparkSession.sql(s"set spark.sql.shuffle.partitions=$defaultPartitionSize")
    import sparkSession.implicits._
    val sspDS: Dataset[SspFullSchema] = sparkSession.sql(
      s"""select
                   trim(_c0) as   rawid,
                   trim(_c1) as   time_stamp,
                   trim(_c2) as   request_id,
                   trim(_c3) as   user_id,
                   Int(_c4) as   publisherid,
                   trim(_c5) as   domain,
                   Int(regexp_extract(_c6, '(\\\\d+)', 1)) as banner_id,
                   Int(_c7) as   variant_id,
                   trim(_c8) as   client_ip_address,
                   trim(_c9) as   country,
                   trim(_c10) as   user_agent_string,
                   trim(_c11) as   referrer_url,
                   Int(_c12) as   width,
                   Int(_c13) as   height,
                   trim(_c14) as   score,
                   trim(_c15) as   deal_id,
                   trim(_c16) as   dsp,
                   Double(_c17) as   original_price,
                   Double(_c18) as   bid,
                   trim(_c19) as   advertiser,
                   trim(_c20) as   deal_type,
                   Double(_c21) as   floor_price,
                   trim(_c22) as   dsp_user_id,
                   trim(_c23) as   sever_location,
                   trim(_c24) as   auction_type,
                   Int(_c25) as   auction_win,
                   trim(_c26) as   result,
                   trim(_c27) as   status,
                   trim(_c28) as   system,
                   trim(_c29) as   no_bid_message,
                   trim(_c30) as   dsp_request_id,
                   trim(_c31) as   dsp_response_time,
                   trim(_c32) as   ttd_id,
                   trim(_c33) as   request_type,
                   trim(_c34) as   traffic_type,
                   Int(_c35) as   device_type,
                   Int(_c36) as   dma,
                   trim(_c37) as   city,
                   trim(_c38) as   browser,
                   Double(_c39) as   open_market_floor_price,
                   Int(_c40) as   iiq_status_code,
                   date_format(to_date(_c1,'MM/dd/yyyy'),'yyyyMMdd') as dt,
                   abs(hash(_c2)) % ($defaultPartitionSize) hash_partition
                   from ssp_df""")
      .as[SspFullSchema]
    sspDS
  }
}