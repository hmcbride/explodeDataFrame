import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, udf}

object DynamicRename {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Dynamic Rename")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._


    val df = Seq(
      ("Airi", "Satou", "Accountant", "Tokyo", "28th Nov 08"),
      ("Angelica", "Ramos", "Chief Executive Officer (CEO)", "London", "9th Oct 09"),
      ("Ashton", "Cox", "Junior Technical Author", "San Francisco", "12th Jan 09"),
      ("Bradley", "Greer", "Software Engineer", "London", "13th Oct 12"),
      ("Brenden", "Wagner", "Software Engineer", "San Francisco", "7th Jun 11"),
      ("Brielle", "Williamson", "Integration Specialist", "New York", "2nd Dec 12"),
      ("Bruno", "Nash", "Software Engineer", "London", "3rd May 11"),
      ("Caesar", "Vance", "Pre-Sales Support", "New York", "12th Dec 11"),
      ("Cara", "Stevens", "Sales Assistant", "New York", "6th Dec 11"),
      ("Cedric", "Kelly", "Senior Javascript Developer", "Edinburgh", "29th Mar 12")
    ).toDF("abc_T_def", "abc_T_def_T_hig", "qwe_T_rty_T_uio_T_plk", "abcx", "abc_T_defk_T_higj")

    val tok = "_T_"

    df.show()

   // val columnsRenamed = df.schema.fieldNames.map(cn => cn.stripPrefix(cn.split(tok)(0)+tok))
    val columnsRenamed = df.schema.fieldNames.map(cn => cn.stripSuffix(tok+cn.split(tok).last))

    val secondDF = df.toDF(columnsRenamed: _*)

    secondDF.show()


    spark.stop()

  }
}


