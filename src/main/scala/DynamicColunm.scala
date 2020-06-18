import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DynamicColunm {

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

    // parameters
   val before = false
   val sufperfix = "_suffix"

    // get the columns
    val columnsRenamed = df.schema.fieldNames

    // rename the columns and create dataframe with new headers
    val nCol = columnsRenamed.map(x => tileValue(before,x, sufperfix))
    val secondDF = df.toDF(nCol: _*)

    secondDF.show()


    spark.stop()

  }


  private def tileValue(m_before: Boolean, origcol: String, m_sufperfix: String) = {
    var colrename = ""
    if (m_before) {
      colrename =  m_sufperfix + origcol
    }
    else {
      colrename =  origcol+  m_sufperfix
    }

    colrename
  }


}


