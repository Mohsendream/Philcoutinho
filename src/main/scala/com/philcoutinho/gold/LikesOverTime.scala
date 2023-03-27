package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object LikesOverTime {

  def likesOverTime(likes: DataFrame, spark: SparkSession): DataFrame = {
    likes.createOrReplaceTempView("likesTable")
    val result = spark.sql("SELECT edge_media_preview_like.count as Likes , taken_at_timestamp as Date , username as postOwner from likesTable")
    result
  }
}
