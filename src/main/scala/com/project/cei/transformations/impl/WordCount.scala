package com.project.cei.transformations.impl

import com.project.cei.transformations.Transform
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount extends Transform{
  override def transformation(spark:SparkSession,context: SparkContext, sqlContext: SQLContext, jobType: String,input:String,output:String): Unit = {

    import spark.implicits._

    val data: Dataset[String] = sqlContext.read.textFile(input)

    val words: Dataset[String] = data
      .map(WordCount.cleanData)
      .flatMap(WordCount.tokenize)
      .filter(_.nonEmpty)

    val wordFrequencies: DataFrame = words
      .map(WordCount.keyValueGenerator)
      .rdd.reduceByKey(_ + _)
      .toDF("word", "frequency")

    wordFrequencies
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header","true").save(output)

  }
  def cleanData(line: String): String = {
    line
      .toLowerCase
      .replaceAll("[,.]"," ")
      .replaceAll("[^a-z0-9\\s-]","")
      .replaceAll("\\s+"," ")
      .trim
  }

  def tokenize(line: String): List[String] = {
    line.split("\\s").toList
  }

  def keyValueGenerator(word: String): (String, Int) = {
    (word, 1)
  }
}
