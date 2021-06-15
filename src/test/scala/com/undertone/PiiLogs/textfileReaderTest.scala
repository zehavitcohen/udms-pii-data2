package com.undertone.PiiLogs

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class textfileReaderTest {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(getClass.getName)
    .master("local[4]")
    .getOrCreate()


  val filePath = "s3://udmp-configurations-prod/udmp-ssp-deals-scala/environment.ini"

  val config : Config = EnvironmentUtils.loadConfig(filePath, "us-east-1")

  val dirtyDates: String =  "20210509,20210510"

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
  val dayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val currentTimeStamp: String = LocalDateTime.now().format(formatter)
  val currentDate: Timestamp = Timestamp.valueOf(s"${LocalDateTime.now().format(dayFormatter)} 00:00:00")

  val requestFunnel: List[RequestFunnel] = List(
    RequestFunnel("2021-06-06 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
    //should be filtered because of Device_Id =['N/A']
    RequestFunnel("2021-06-06 12:09:30.785":Timestamp, "N/A":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
    //should be filtered because different dt
    RequestFunnel("2021-06-08 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "delete":String,  "N/A":String)
  )

  val requestFunnelDS: Dataset[RequestFunnel] = sparkSession.sparkContext.parallelize(requestFunnel).toDS()
  val dtString:String = "20210509"

  @Test
  def execute_explosion_test():Unit = {


    requestFunnelDS.show()

    requestFunnelDS.createOrReplaceTempView("request_funnel")

    val requestFunnelTableName:String = "request_funnel"

    val aggSSPExplodedByDeal: Dataset[AggSSPDealsExploded] = execute_explosion(sparkSession,
      requestFunnelTableName,
      dirtyDates)

    aggSSPExplodedByDeal.show(100)

    val expectedResult: Set[AggSSPDealsExploded] = Set(
      AggSSPDealsExploded("2021-06-06 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
      AggSSPDealsExploded("2021-06-06 12:09:30.785":Timestamp, "N/A":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
      AggSSPDealsExploded("2021-06-08 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "delete":String,  "N/A":String)
    )


    val actualResult: Set[AggSSPDealsExploded] = aggSSPExplodedByDeal.collect().toSet

    Assert.assertEquals(expectedResult,actualResult)

    val aggregatedExpectedResult:Set[AggSSPDeal] = Set(
      AggSSPDeal("2021-06-06 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
      AggSSPDeal("2021-06-06 12:09:30.785":Timestamp, "N/A":String, "false":String, "access":String,  "zcohen@Undertone.com":String),
      AggSSPDeal("2021-06-08 12:09:30.785":Timestamp, "2fcedd34f175424ca3225fad3c348ecd":String, "false":String, "delete":String,  "N/A":String)
    )

    val aggSSPDeals: Dataset[AggSSPDeal] = execute_aggregation(sparkSession,aggSSPExplodedByDeal)

    val aggregatedActualResult:Set[AggSSPDeal] = aggSSPDeals.collect().toSet

    Assert.assertEquals(aggregatedExpectedResult,aggregatedActualResult)

  }

  //    @Test
  //    def reading_test():Unit={
  //
  //
  //      val df:DataFrame  = sparkSession.read.parquet("src/test/scala/com/undertone/files/part-00003-e60e6008-3eb3-4d08-8547-4fa31c24c180.c000.snappy.parquet")
  //
  //      df.createOrReplaceTempView("req")
  //
  //      df.printSchema()
  //
  //      val aggSSPExplodedByDeal: Dataset[AggSSPDealsExploded] = execute_explosion(sparkSession,
  //        "req",
  //        dirtyDates)
  //
  //      aggSSPExplodedByDeal.show(100)
  //    }

}

