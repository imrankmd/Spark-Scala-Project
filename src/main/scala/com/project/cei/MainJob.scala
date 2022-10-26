package com.project.cei

import com.project.cei.transformations.impl.{MovieAnalysis, WordCount}
import com.project.cei.utilities.{SparkConnection, TransformationType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory
object MainJob {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
  if (args.length != 3) {
    System.exit(0)
  }
    val jobType = args(0)
    val input = args(1)
    val output = args(2)


  val spark = SparkConnection.getSparkContext
  val sqlcontext = spark.sqlContext
  val sparkcontext = spark.sparkContext


  Transform(jobType, sparkcontext, sqlcontext,spark,input,output)

}

def Transform(jobType: String, context: SparkContext, sqlContext: SQLContext,spark:SparkSession,input:String,output:String): Unit = {

  jobType.toLowerCase match {
  case TransformationType.WordCount => {
  logger.info("Transformation Started for " + jobType)
  WordCount.transformation(spark,context, sqlContext,jobType,input,output)
}
  case TransformationType.MovieAnalysis => {
  logger.info("Transformation Started for " + jobType)
    MovieAnalysis.transformation(spark,context, sqlContext,jobType,input,output)
}
  case _ => {
  logger.error("Invalid Transformation Type" + jobType)
  System.exit(-1)
}
}
}
}
