package main
import sqlContext.implicits._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Country {
  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Country CSV").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
val csvpath = args(0)
val csv = sc.textFile(csvpath)  //reading CSV
val rows = csv.map(line => line.split(",").map(_.trim)) 
val header = rows.first
val data = rows.filter(_(0) != header(0)) //removing header 
val csvdf = data.map({case Array(a,b)=>(a,b)}).toDF("country","values") //Creating dataframe from rdd

csvdf.write.parquet("hdfs://quickstart.cloudera:8020/rishabh/country") //writing parquet to HDFS

val parq = sqlContext.read.parquet("hdfs://quickstart.cloudera:8020/rishabh/country/part-r-00000-38927791-9a35-447f-93c5-5c633bb8a759.gz.parquet")
//reading parquet

val splitparq = parq
.withColumn("col1", split(col("values"), ";").getItem(0))
.withColumn("col2", split(col("values"), ";").getItem(1))
.withColumn("col3", split(col("values"), ";").getItem(2))
.withColumn("col4", split(col("values"), ";").getItem(3))
.withColumn("col5", split(col("values"), ";").getItem(4))
.groupBy($"country").agg(sum($"col1"),sum($"col2"),sum($"col3"),sum($"col4"),sum($"col5")).select($"country",concat_ws(";",$"sum(col1)",$"sum(col2)",$"sum(col3)",$"sum(col4)",$"sum(col5)").alias("values")).orderBy($"country")
//splitting the columns using ";" and aggregating the column values while grouping by country

/*+-------+---------+---------+---------+---------+---------+                     
|country|sum(col1)|sum(col2)|sum(col3)|sum(col4)|sum(col5)|
+-------+---------+---------+---------+---------+---------+
|  India|    246.0|    153.0|    148.0|    100.0|     90.0|
| Canada|    183.0|    258.0|    150.0|    263.0|     71.0|
|England|    178.0|    114.0|    175.0|    173.0|    153.0|
|  China|    218.0|    239.0|    234.0|    209.0|     75.0|
|Germany|    144.0|    166.0|    151.0|    172.0|     70.0|
+-------+---------+---------+---------+---------+---------+*/

/*scala> splitparq.show(20,false)
/*
+-------+-----------------------------+                                         
|country|values                       |
+-------+-----------------------------+
|Canada |183.0;258.0;150.0;263.0;71.0 |
|China  |218.0;239.0;234.0;209.0;75.0 |
|England|178.0;114.0;175.0;173.0;153.0|
|Germany|144.0;166.0;151.0;172.0;70.0 |
|India  |246.0;153.0;148.0;100.0;90.0 |
+-------+-----------------------------+
*/

splitparq.repartition(5)  //repartition the DF
splitparq.write.parquet("hdfs://quickstart.cloudera:8020/rishabh/country/final/") //writing to HDFS

}
}
