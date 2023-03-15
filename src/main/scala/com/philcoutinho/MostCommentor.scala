package com.philcoutinho

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.philcoutinho.Comments.comments

object MostCommentor {

  def mostcommentor(df: DataFrame): DataFrame = {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("comment_Test")
      .getOrCreate()
    val commentstab = comments(df)
    commentstab.createOrReplaceTempView("my_table")
    val result = spark.sql("SELECT  owner_id,comment_username, COUNT(*) as nbr FROM my_table GROUP BY owner_id,comment_username")
    result
  }
}
