package com.project.cei.transformations

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait Transform {
  def transformation(spark: SparkSession,context: SparkContext, sqlContext: SQLContext,jobType:String,input:String,output:String)
}
