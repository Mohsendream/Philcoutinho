package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object CommentsInTime {

  def commentsInTime(CommentData: DataFrame, spark: SparkSession): DataFrame = {

    CommentData.createOrReplaceTempView("my_table")
    val result = spark.sql("SELECT typename, created_at, comment_id, owner_id, comment_username, text " +
      " FROM my_table WHERE FROM_UNIXTIME (created_at) BETWEEN ('2020-01-01') AND ('2021-05-16')")
    result
  }
}
