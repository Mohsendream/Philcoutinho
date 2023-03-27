package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaCommentCount {

  def mediaCommentCount(EdgeMediaComment: DataFrame, spark: SparkSession): DataFrame = {

    EdgeMediaComment.createOrReplaceTempView("EdgeMediaComment")
    val result = spark.sql("SELECT typename, SUM(edge_media_to_comment.count) as count FROM EdgeMediaComment GROUP BY typename")
    result
  }
}
