package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaPreviewLike {

  def mediapreviewlike(EdgeMediaPreview: DataFrame, spark: SparkSession): DataFrame = {
    EdgeMediaPreview.createOrReplaceTempView("EdgeMediaPreview")
    val result = spark.sql("SELECT __typename, SUM(edge_media_preview_like.count) as count FROM EdgeMediaPreview GROUP BY  __typename ")
    result
  }
}
