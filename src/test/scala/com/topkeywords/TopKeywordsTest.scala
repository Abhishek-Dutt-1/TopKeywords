package com.topkeywords

import org.apache.spark.sql.SparkSession
import org.scalatest._

class TopKeywordsTest extends FlatSpec with Matchers with BeforeAndAfter {

  var spark:SparkSession = _

  before {

    spark = SparkSession
      .builder()
      .master("local")
      .appName("TopKeywords")
      .config("spark.executor.memory", "2G")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

  }

  after {
    spark.stop()
  }

  behavior of "TopKeywords"

  it should "trim whitespaces from keywords" in {

    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, " keyword1 "), (2, " keyword2 ")))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword")
    val df1 = TopKeywords.preProcessData(df)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword1"),
      Array(2, "keyword2"))
  }

  it should "convert keywords to lower case" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "KEYWORD1"), (2, "KEYWORD2")))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword")
    val df1 = TopKeywords.preProcessData(df)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword1"),
      Array(2, "keyword2"))
  }

  it should "calculates keyword counts per category" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "keyword1"), (1, "keyword1"), (2, "keyword1"), (3, "keyword3")))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword")
    val df1 = TopKeywords.getKeywordCountsByCategory(df)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword1", 2),
      Array(2, "keyword1", 1),
      Array(3, "keyword3", 1))
  }

  it should "calculates keyword ranks by counts per category" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "keyword1", 2), (1, "keyword2", 7), (2, "keyword1", 9), (3, "keyword3", 1)))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword", "count")
    val df1 = TopKeywords.getTopKeywordsByRank(df, 10)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword2", 7),
      Array(1, "keyword1", 2),
      Array(2, "keyword1", 9),
      Array(3, "keyword3", 1))
  }

  it should "concat keywords and counts as keyword_count for each keyword" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "keyword1", 2), (1, "keyword2", 7), (2, "keyword1", 9), (3, "keyword3", 1)))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword", "count")
    val df1 = TopKeywords.concatKeywordCounts(df)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword1", 2, "keyword1_2"),
      Array(1, "keyword2", 7, "keyword2_7"),
      Array(2, "keyword1", 9, "keyword1_9"),
      Array(3, "keyword3", 1, "keyword3_1"))
  }

  it should "concat keyword_count's for each categoryId" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "keyword1", 2, "keyword1_2"), (1, "keyword2", 7, "keyword2_7"), (2, "keyword1", 9, "keyword1_9"), (3, "keyword3", 1, "keyword3_1")))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keyword", "count", "keyword_count")
    val df1 = TopKeywords.concatKeywords(df)
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array(1, "keyword1_2,keyword2_7"),
      Array(2, "keyword1_9"),
      Array(3, "keyword3_1"))
  }

  it should "concat keyword_count's with categoryId" in {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(Seq((1, "keyword1_2,keyword2_7"), (2, "keyword1_9"), (3, "keyword3_1")))
    val df = spark.createDataFrame(rdd).toDF("categoryId", "keywords_count")
    val df1 = TopKeywords.concatCategoryIdKeywords(df)
    df1.show()
    val output = df1.collect().map(row => row.toSeq)
    output should contain allOf (
      Array("1|keyword1_2,keyword2_7"),
      Array("2|keyword1_9"),
      Array("3|keyword3_1"))
  }



}