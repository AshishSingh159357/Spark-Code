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


    // Creating Temp View For Business Details
    B1.createOrReplaceTempView("EBD")

    // Creating Temp View For Personal Details
    P1.createOrReplaceTempView("EPD")



    // Display average ,minimum ,maximum salary for employee between 30 and 40
    var sql1 = sparkSession.sql("SELECT avg(salary) as AverageSalary,min(salary) as MinSalary,max(salary) as MaxSalary FROM EPD inner join EBD on EPD.Emp_ID=EBD.Emp_ID where EPD.AgeinYrs between 30 and 40")
    sql1.show()


    // Display number of employee join at particular year
    var sql2 = sparkSession.sql("SELECT count(Emp_ID),Year_of_Joining FROM EBD group by Year_of_Joining order by Year_of_Joining")
    sql2.show()








   // var sql3 = sparkSession.sql("SELECT count(Emp_ID),Year_of_Joining FROM EBD group by Year_of_Joining order by Year_of_Joining")
 //B1.collect()

   // print("----------------------------------------")



   // B1.foreach(row=>{
    //  var a=row.getAs("Salary")
    //  print(a.asInstanceOf[Int]+4)


   // })



  }

}
