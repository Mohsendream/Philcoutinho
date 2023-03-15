package com.philcoutinho

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.philcoutinho.Comments.comments

object MostCommentsInTime {

  def commentsInTime(df : DataFrame):DataFrame={
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("comment_Test")
      .getOrCreate()
    val timetable = comments(df)
    timetable.createOrReplaceTempView("my_table")
    val result = spark.sql("SELECT __typename, created_at, comment_id, owner_id, comment_username, text " +
      " FROM my_table WHERE FROM_UNIXTIME (created_at) BETWEEN ('2020-01-01') AND ('2021-05-16')")
    result
  }
}
