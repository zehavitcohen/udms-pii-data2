package com.undertone

class PiiLogsAggregatorTest {

  @Test
  def requestsLogsTests(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/req/").getFile)

    LogsTransformationUtil.processLogsType1(sparkSession, 0, 4, 5, input, "requests.json", SaveMode.Overwrite)
  }

  @Test
  def clicksLogsTests(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/clk/").getFile)

    LogsTransformationUtil.processLogsType1(sparkSession, 0, 4, 5, input, "clicks.json", SaveMode.Overwrite)
  }

  @Test
  def eventsLogsTests(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/evt/").getFile)

    LogsTransformationUtil.processLogsType1(sparkSession, 0, 3, 4, input, "events.json", SaveMode.Overwrite)
  }

  @Test
  def impressionsLogsTests(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/imp/").getFile)

    LogsTransformationUtil.processLogsType1(sparkSession, 0, 15, 16, input, "impressions.json", SaveMode.Overwrite)
  }

  @Test
  def testParameters(): Unit = {
//    val envIniList: List[String] = List("staging", "production")
val envIniList: List[String] = List("staging")
    for (env <- envIniList) {
      new Parameters(env)
    }
  }
}
