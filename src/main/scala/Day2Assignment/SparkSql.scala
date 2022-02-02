package Day2Assignment

import org.apache.spark
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession




object SparkSql {

  def main(args: Array[String]): Unit={
    val sparkSession=SparkSession.builder().master(master="local")
      .appName(name="this is my first spark program with scala language")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","file:////usr/local/spark-3.2.1-bin-hadoop3.2/Extra-Spark/sprak-events")
      .config("spark.history.fs.logDirectory","file:////usr/local/spark-3.2.1-bin-hadoop3.2/Extra-Spark/sprak-events")
      .getOrCreate()

    var B1= sparkSession.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/dataset/Employee_Business_Details.csv")
    var P1= sparkSession.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/dataset/Employee_personal_details.csv")

  //  B1.show()
    //P1.show()

    B1.createOrReplaceTempView("EBD")
    P1.createOrReplaceTempView("EPD")





  var sql1 = sparkSession.sql("SELECT * FROM EBD")
 sql1.show()

    var sql2= sparkSession.sql("SELECT * FROM EPD")
   sql2.show()







    // df=df.filter(df("Place_Name")==="Sodus")
    // df.show()
    // val dff= sparkSession.read.format("csv").option("header","true").load("hdfs://localhost:9000/dataset/Test_address_details.csv")
    // dff.show()

  }

}
