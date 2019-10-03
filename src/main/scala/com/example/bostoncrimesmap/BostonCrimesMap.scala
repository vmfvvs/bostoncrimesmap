package com.example.bostoncrimesmap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object BostonCrimesMap extends App {

  val crime_csv: String = args(0)
  val offense_codes_csv: String = args(1)
  val output_parquet: String = args(2)

  val  sparkq = SparkSession.builder().appName("BostonCrimesMap").getOrCreate()

  import sparkq.implicits._

  val crime_df = sparkq.read.option("header","true").option("inferSchema","true").csv(crime_csv).filter($"DISTRICT".isNotNull).na.fill(0.0)
  val offense_df = sparkq.read.option("header","true").option("inferSchema","true").csv(offense_codes_csv).withColumn("crime_type",trim(split($"NAME", "-")(0)))

  val offense_br = broadcast(offense_df)



  val join_tables = crime_df
    .join(offense_br, $"CODE" === $"OFFENSE_CODE")

   .select(crime_df("INCIDENT_NUMBER"), crime_df("DISTRICT"), crime_df("MONTH"), crime_df("Lat"), crime_df("Long"), offense_br("crime_type"))




  val crimes_count_prep = crime_df
    .groupBy($"DISTRICT")
    .count()

  crimes_count_prep.createOrReplaceTempView("crimes_vew")
  val crimes_count = sparkq.sql("SELECT  DISTRICT, count as crimes_total FROM crimes_vew")


  val crimes_median_prep = crime_df
    .groupBy($"DISTRICT", $"MONTH")
    .count()


  crimes_median_prep.createOrReplaceTempView("median_vew")
  val crimes_median = sparkq.sql("SELECT  DISTRICT, percentile_approx(count, 0.5) as crimes_monthly FROM median_vew group by DISTRICT")//.show()


  val w_function: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"Crime_District".desc)
  def my_function: UserDefinedFunction = udf((x: mutable.WrappedArray[String]) => x.toList.mkString(", "))

  val frequent_crime_types = join_tables
    .groupBy($"DISTRICT", $"crime_type")
    .agg(count($"INCIDENT_NUMBER").alias("Crime_District"))
    .withColumn("row_number", row_number().over(w_function))
    .filter($"row_number" < 4)
    .drop($"row_number")
    .groupBy($"DISTRICT")
    .agg(collect_list($"crime_type").alias("cr"))
    .withColumn("frequent_crime_types", my_function($"cr"))
    .drop($"cr")



  val crimes_lat_lng = crime_df
    .groupBy($"DISTRICT")
    .agg(mean($"Lat").alias("lat"),
      mean($"Long").alias("lng"))

  val all= crimes_count
    .join(crimes_median, Seq("DISTRICT"))
    .join(frequent_crime_types, Seq("DISTRICT"))
    .join(crimes_lat_lng, Seq("DISTRICT"))


   all.repartition(1).write.parquet(output_parquet)

    /*
     result.coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("C:\\Teech\\crimes-in-boston\\10")
*/

  sparkq.stop()

}
