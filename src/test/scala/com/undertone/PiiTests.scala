package com.undertone

class PiiTests {

  @Test
  def sspLogsTests(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/ssp/").getFile)
    val dsInput = LogsTransformationUtil.getSspDS(sparkSession, input, 200)
    val sspJsonDs: Dataset[SspJsonSchema] = SspLogsAggregator.processLogsSSP(sparkSession, dsInput)
    val countDsInput = dsInput.count()
    val sumSspJson = sspJsonDs.agg(sum("totalRequests")).first().getLong(0)
    println("countDsInput: " + countDsInput)
    println("sumSspJson: " + sumSspJson)
    Assert.assertTrue(countDsInput>=0 && sumSspJson>0 && countDsInput == sumSspJson)
  }

  @Test
  def sspConversionTest(): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    val input: List[String] = List(getClass.getResource("/ssp/").getFile)
    val dsInput = LogsTransformationUtil.getSspDS(sparkSession, input, 200)
    SspLogsConverter.convertSspToParquet(sparkSession, dsInput, "ssp.parquet", SaveMode.Overwrite, 200)
  }
}