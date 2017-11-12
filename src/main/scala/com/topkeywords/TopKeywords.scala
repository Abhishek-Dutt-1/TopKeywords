package com.topkeywords

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TopKeywords {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("TopKeywords")
    .config("spark.executor.memory", "2G")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]) {

    val sourceFile = "src/main/resources/data.txt"
    val outputFolder = "output/"
    val maxRank = 10

    print("Clearing output folder.")
    deleteFolder(new File(outputFolder))

    val tmpDF = loadData(sourceFile)

    val df = preProcessData(tmpDF)

    val groupedDF = getKeywordCountsByCategory(df)

    val rankedDF = getTopKeywordsByRank(groupedDF, maxRank)

    val concatDF1 = concatKeywordCounts(rankedDF)

    val concatDF2 = concatKeywords(concatDF1)

    val concatDF3 = concatCategoryIdKeywords(concatDF2)

    writeOutput(concatDF3, outputFolder)

    spark.stop()

  }


  /**
    * Loads a csv file as Dataframe
    * @param fileName
    * @return
    */
  def loadData(fileName: String) = {
    spark.read
      .option("header", "false")
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
      .csv(fileName )
  }

  /**
    * Add column names and lowercase and trim whitespaces from columns
    *
    * @param df: DataFrame
    * @return DataFrame ("categoryId", "keyword")
    */
  def preProcessData(df: DataFrame) = {
    df.toDF("categoryId", "keyword").withColumn("keyword", trim($"keyword")).withColumn("keyword", lower($"keyword"))
  }

  /**
    * Add column containing counts for each keyword
    *
    * @param df: DataFrame("categoryId", "keyword")
    * @return DataFrame("categoryId", "keyword", "count")
    */
  def getKeywordCountsByCategory(df: DataFrame) = {
    df.groupBy("categoryId", "keyword").count().sort($"categoryId", $"count".desc, $"keyword")
  }

  /**
    * Add column containing rank of each keyword by count per category, drops rows with rank higher than the given cutoff rank
    *
    * Use row_number to strictly keep 10 keywords per category
    *
    * Use dense_rank to respect ties in keywords counts (may result in more than 10 keywords per category)
    *
    * @param df: DataFrame("categoryId", "keyword", "count")
    * @param maxRank : Maximum rank of keywords to keep
    * @return DataFrame("categoryId", "keyword", "count")
    */
  def getTopKeywordsByRank(df: DataFrame, maxRank: Int) = {
    val w = Window.partitionBy($"categoryId").orderBy($"count".desc)
    val ranked = df.withColumn("rank", row_number.over(w))
      //.withColumn("rank", dense_rank.over(w))
      .filter("rank <= " + maxRank)
      .sort($"categoryId", $"rank")
      .drop("rank")
    ranked
  }

  /**
    * Concat keyword_count
    *
    * @param df: DataFrame("categoryId", "keyword", "count")
    * @return DataFrame("categoryId", "keyword", "count", "keyword_count")
    */
  def concatKeywordCounts(df: DataFrame) = {
    df.show()
    df.withColumn("keyword_count", concat_ws("_", $"keyword", $"count"))
  }

  /**
    * Concat keyword_count1, keyword_count2
    *
    * @param df: DataFrame("categoryId", "keyword", "count", "keyword_count")
    * @return DataFrame("categoryId", "keyword", "count", "keyword_count", "keywords_count")
    */
  def concatKeywords(df: DataFrame) = {
    df.show()
    df.groupBy("categoryId").agg(concat_ws(",", collect_list($"keyword_count")) as "keywords_count")
  }

  /**
    * Concat categoryId|keyword_count1, keyword_count2, ...
    *
    * @param df: DataFrame("categoryId", "keyword", "count", "keyword_count", "keywords_count")
    * @return DataFrame("top_keywords")
    */
  def concatCategoryIdKeywords(df: DataFrame) = {
    df.show()
    df.withColumn("top_keywords", concat_ws("|", $"categoryId", $"keywords_count"))
      .sort($"categoryId")
      .drop($"categoryId")
      .drop($"keywords_count")
  }

  /**
    * Writes DataFrame as csv in the given folder
    *
    * @param df: DataFrame("top_keywords")
    * @param outputFolder
    * @return Unit
    */
  def writeOutput(df:DataFrame, outputFolder: String) = {
    df.show()
    df.repartition(1).write.text(outputFolder)
  }

  /**
    * Clear output folder
    * @param file
    * @return
    */
  def deleteFolder(file: File): Array[(String, Boolean)] = {
    Option(file.listFiles).map(_.flatMap(f => deleteFolder(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
  }

}
