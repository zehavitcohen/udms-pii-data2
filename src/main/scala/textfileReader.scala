// Each library has its significance, I have commented when it's used
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row


object textfileReader {

  val conf = new SparkConf().setAppName("textfileReader")
  conf.setMaster("local")

  // Using above configuration to define our SparkContext
  val sc = new SparkContext(conf)

  // Defining SQL context to run Spark SQL
  val sqlContext = new SQLContext(sc)

  def main (args:Array[String]): Unit = {

    // Reading the text file
    val squidString = sc.textFile("pii_20210606.txt")

    // Defining the data-frame header structure & schema
    val squidHeader = "Date Id Flag Action User_Email"
    val schema = StructType(squidHeader.split(" ").map(fieldName => StructField(fieldName,StringType, true)))
    // Converting String RDD to Row RDD for 5 attributes
    val rowRDD = squidString.map(_.split(" ")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5))

    // Creating data-frame based on Row RDD and schema
    val squidDF = sqlContext.createDataFrame(rowRDD, schema)

    // Saving as temporary table
    squidDF.registerTempTable("RequestedAction")

    // Retrieving all the records
    val allrecords = sqlContext.sql("select * from RequestedAction")

    // Showing top 5 records with false truncation i.e. showing complete row value
    allrecords.show(5,false)

    allrecords.write.saveAsTable("allrecords")

    // Printing schema before transformation
    allrecords.printSchema()

    // Something like this for date, integer and string conversion

    // To have multiline sql use triple quotes
    val transformedData = sqlContext.sql("""
        select cast (Id as string) as Id
        from allrecords
        where Action = 'access'
        """)

    // To print schema after transformation, you can see new fields data types
    transformedData.printSchema()
    transformedData.show()

    sc.stop()
  }
}