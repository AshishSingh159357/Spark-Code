package Day2Assignment
/*
@ author ashish.singh@stltech.in
@ version 1.0
@ date 3/2/2022
@ copyright Sterlite Technologies Ltd. All right reserved
@ description Spark Sql
*/

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


    // Loading Bussiness Data
    var B1= sparkSession.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/dataset/Employee_Business_Details.csv")

    // Loading Personal Data
    var P1= sparkSession.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/dataset/Employee_personal_details.csv")


    var A1= sparkSession.read.format("csv").option("header","true")
      .load("hdfs://localhost:9000/dataset/employee_address_details.csv")



    // Creating Temp View For Business Details
    B1.createOrReplaceTempView("EBD")

    // Creating Temp View For Personal Details
    P1.createOrReplaceTempView("EPD")

    // Creating Temp View For Address Details
    A1.createOrReplaceTempView("EAD")



    // Display average ,minimum ,maximum salary for employee between 30 and 40
    var sql1 = sparkSession.sql("SELECT avg(salary) as AverageSalary,min(cast(salary as int)) as MinSalary,max(cast(salary as int)) as MaxSalary FROM EPD inner join EBD on EPD.Emp_ID=EBD.Emp_ID where EPD.AgeinYrs between 30 and 40")
    sql1.show()



    // Display number of employee join at particular year
    var sql2 = sparkSession.sql("SELECT count(Emp_ID),Year_of_Joining FROM EBD group by Year_of_Joining order by Year_of_Joining")
    sql2.show()




    // Display the Last Salary of Employee before Last Hike
    var sql3 = sparkSession.sql("SELECT Emp_ID,cast(Salary as float),LastHike,(Salary*100)/(cast( replace(LastHike,'%','')  as float) + 100 ) as LastSalary FROM EBD")
    sql3.show()

    // Storing the Last Salary Data to HDFS
    sql3.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/LastSalary")




    // Display the Average Weight of Employee join in Monday or Friday or Wednesday
    var sql4 = sparkSession.sql("SELECT avg(WeightinKgs) FROM EPD inner join EBD on EPD.Emp_ID=EBD.Emp_ID where DOW_of_Joining='Monday' or DOW_of_Joining='Wednesday' or DOW_of_Joining='Friday' ")
    sql4.show()

    // Storing the Average Weight of Employee Data to HDFS
    sql4.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/AverageWeight")




  }

}
