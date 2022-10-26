package com.project.cei.transformations.impl

import com.project.cei.transformations.Transform
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object MovieAnalysis extends Transform{
  override def transformation(spark:SparkSession,context: SparkContext, sqlContext: SQLContext, jobType: String, input:String, output:String): Unit = {

    val DF = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("delimiter",",").load(input)
    DF.createOrReplaceTempView("rawDF")
    DF.printSchema()

    val FinalDF=DF.withColumn("rank", rank().over(Window.partitionBy("Genre").orderBy(col("ratings").desc))).filter("rank <= 10")
    FinalDF.write.format("com.databricks.spark.csv").option("delimiter","|").option("header","true").save(output)

  }
}
