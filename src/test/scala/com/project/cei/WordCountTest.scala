package com.project.cei

import java.util.Properties

import com.project.cei.transformations.impl.WordCount
import org.apache.log4j.{LogManager, Logger, PropertyConfigurator}
import org.apache.spark.sql.{Dataset, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers



abstract class WordCountTest  extends AnyFunSuite with BeforeAndAfterAll with Matchers{

  val JOB_NAME: String = "Word Count Test Job"
  val LOGGER_PROPERTIES: String = "log4j-test.properties"
  val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  val TEST_INPUT: String = this.getClass.getClassLoader.getResource("dummyText.txt").getPath
  var spark: SparkSession = _

  def setupLogger(): Unit = {
    val properties = new Properties
    properties.load(getClass.getClassLoader.getResource(LOGGER_PROPERTIES).openStream())
    LogManager.resetConfiguration()
    PropertyConfigurator.configure(properties)
  }

  private object implicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  override def beforeAll: Unit = {
    setupLogger()
    LOG.info("Setting up spark session")
    spark = SparkSession.builder().appName(JOB_NAME).master("local[*]").getOrCreate()
  }

  test("Test data cleaning") {
    assertResult("this is text with 123 from spark-wordcount")(WordCount.cleanData("$This is text, with 123, from Spark-WordCount."))
  }

  test("Test tokenizer") {
    List("tokenized","this","is") should contain theSameElementsAs WordCount.tokenize("this is tokenized")
  }

  test("Test key value generator") {
    assertResult(("test", 1))(WordCount.keyValueGenerator("test"))
  }

  test("Verify no data is dropped") {
    import implicits._

    val data: Dataset[String] = spark.read.textFile(TEST_INPUT)

    val words: Dataset[String] = data
      .map(WordCount.cleanData)
      .flatMap(WordCount.tokenize)
      .filter(_.nonEmpty)
    assertResult(1000L)(words.count())
  }

  override def afterAll: Unit = {
    LOG.info("Closing spark session")
    spark.close()
  }
}
