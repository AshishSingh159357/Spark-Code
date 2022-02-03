package Day1Assignment

import org.apache.spark.sql.SparkSession

object FileReadFromHdfs {
  def main(args: Array[String]): Unit={
    val sparkSession=SparkSession.builder().master(master="local")
      .appName(name="this is my first spark program with scala language")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","file:////usr/local/spark-3.2.1-bin-hadoop3.2/Extra-Spark/sprak-events")
      .config("spark.history.fs.logDirectory","file:////usr/local/spark-3.2.1-bin-hadoop3.2/Extra-Spark/sprak-events")
      .getOrCreate()

    // To Load the the data
    var df= sparkSession.read.format("csv").option("header","true").load("hdfs://localhost:9000/dataset/employee_address_details.csv")

    // Storing Data with Different Name
    // overwrite mode is to replace the file and create automatic given path on hadoop
    df.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/New_employee_address_details.csv")

    // Display 1000 rows and full column content
    df.show(1000,false)



    // Filter the data with given condition
    val df1=df.filter(df("Region")==="Northeast")

    val df2=df.filter(df("Region")==="South")

    val df3=df.filter(df("Region")==="Midwest")

    val df4=df.filter(df("Region")==="West")

    val df5=df.filter(df("Region")==="East")

    //To show the DataFrame
    df1.show()
    df2.show()
    df3.show()
    df4.show()
    df5.show()

    // This will store data of dataframe to HDFS to given path
    df1.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata/Region=NorthEast/NorthEast.csv")
    df2.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata/Region=South/South.csv")
    df3.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata/Region=Midwest/Midwest.csv")
    df4.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata/Region=West/West.csv")
    df5.write.mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata/Region=East/East.csv")

    //This will Partition or Store the data Region Wise in HDFS to given path
    df.write.option("header",true).partitionBy("Region").mode("overwrite").csv("hdfs://localhost:9000/dataset/empdata-2/Region")


  }

}
