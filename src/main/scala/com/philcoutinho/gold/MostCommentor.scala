package com.philcoutinho.gold


import org.apache.spark.sql.{DataFrame, SparkSession}


object MostCommentor {

  def mostcommentor(CommentData: DataFrame,spark : SparkSession): DataFrame = {

    CommentData.createOrReplaceTempView("my_table")
    val result = spark.sql("SELECT  owner_id,comment_username, COUNT(*) as nbr FROM my_table GROUP BY owner_id,comment_username ORDER BY nbr DESC")
    result
  }
}