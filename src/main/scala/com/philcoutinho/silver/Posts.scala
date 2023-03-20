package com.philcoutinho.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object Posts {

  def posts(InputData: DataFrame): DataFrame = {

    val explodedGraphImage = InputData.select(explode(col("GraphImages")).as("GraphImage"))
    val postsData = explodedGraphImage.select(col("GraphImage.__typename").as("typename"),
      col("GraphImage.comments_disabled").as("comments_disabled"),
      col("GraphImage.dimensions").as("dimensions"),
      col("GraphImage.edge_media_preview_like").as("edge_media_preview_like"),
      col("GraphImage.edge_media_to_caption").as("edge_media_to_caption"),
      col("GraphImage.edge_media_to_comment").as("edge_media_to_comment"),
      col("GraphImage.id").as("id"),
      col("GraphImage.is_video").as("is_video"),
      col("GraphImage.owner").as("owner"),
      col("GraphImage.shortcode").as("shortcode"),
      col("GraphImage.tags").as("tags"),
      col("GraphImage.taken_at_timestamp").as("taken_at_timestamp"),
      col("GraphImage.username").as("username"))
    postsData
  }
}
