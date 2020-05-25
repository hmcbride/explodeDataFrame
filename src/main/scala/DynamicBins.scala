import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object DynamicBins{

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Dynamic Rename")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._


    val df = Seq(
      ("Airi", "Satou", "Accountant", "Tokyo", 75),
      ("Angelica", "Ramos", "Chief Executive Officer (CEO)", "London", 120),
      ("Ashton", "Cox", "Junior Technical Author", "San Francisco", 143),
      ("Bradley", "Greer", "Software Engineer", "London", 160),
      ("Brenden", "Wagner", "Software Engineer", "San Francisco", 300),
      ("Brielle", "Williamson", "Integration Specialist", "New York", 190),
      ("Bruno", "Nash", "Software Engineer", "London", 200),
      ("Caesar", "Vance", "Pre-Sales Support", "New York", 210),
      ("Cara", "Stevens", "Sales Assistant", "New York", 250),
      ("Tara", "Ptevens", " Assistant", "New York", 500),
     ("Cedric", "Kelly", "Senior Javascript Developer", "Edinburgh", 400)
    ).toDF("fn", "ln", "occup", "town", "numval")

    df.show()

    val  c1 = df.select("numval").rdd.map(r => r(0).toString.toInt)
    val bset = List(0,100, 150, 200, 250 , 300, 400,1000 )



    val bset2 = bset.take(bset.size - 1).zip(bset.tail) zip (Stream from 1)

    val rsx =   c1.map(cc => tileValue(cc,bset2))

    rsx.collect().foreach(println)

    // zip the data frame with rdd
    val rdd_new = df.rdd.zip(rsx).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))

    // create a new data frame from the rdd_new with modified schema
    spark.createDataFrame(rdd_new, df.schema.add("tile_num", IntegerType)).show

    spark.stop()

  }


  private def tileValue(cval: Int, bset2:List[((Int, Int), Int)]) = {
    var binval = 0
    for (bval <- bset2) {
      if (cval > bval._1._1 && cval <= bval._1._2) {
        binval = bval._2


      }
    }
    binval
  }
}


