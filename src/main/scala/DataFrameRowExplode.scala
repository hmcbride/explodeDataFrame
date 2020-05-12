import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DataFrameRowExplode {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
                            .appName("Spark Explode Data Frame")
                            .config("spark.master", "local")
                            .getOrCreate();

    import spark.implicits._


    val rangeDF = spark.read.format("csv").option("sep", ",")
                                                  .option("inferSchema", "true")
                                                  .option("header", "true")
                                                  .load("source.csv")
                                                  .withColumn("colRange", createRange($"min_id", $"max_id"))
                                                  .withColumn("num", explode($"colRange"))
                                                  .drop("colRange")

       rangeDF.show(100)



    spark.stop()

  }

  def createRange = udf((min: Int, max: Int) => {
    val r = (min to max).toArray

    r
  })

}