package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaCommentCount {

  def mediacommentcount(EdgeMediaComment: DataFrame, spark: SparkSession): DataFrame = {
    EdgeMediaComment.createOrReplaceTempView("EdgeMediaComment")
    val result = spark.sql("SELECT __typename, SUM(edge_media_to_comment.count) as count FROM EdgeMediaComment GROUP BY __typename")
    result
  }
}
