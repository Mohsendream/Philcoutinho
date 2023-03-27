package com.philcoutinho.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object Comments {

  def comments(InputData: DataFrame): DataFrame = {

    val explodedGraphImage = InputData.select(explode(col("GraphImages")).as("GraphImage"))
    val explodedDataArray = explodedGraphImage.select(explode(col("GraphImage.comments.data")).as("Data"),
      col("GraphImage.username").as("username"),
      col("GraphImage.__typename").as("__typename"))
    val commentsData = explodedDataArray.select(col("__typename").as("typename"),
      col("Data.created_at").as("created_at"),
      col("Data.id").as("comment_id"),
      col("Data.text").as("text"),
      col("Data.owner.id").as("owner_id"),
      col("Data.owner.username").as("comment_username"),
      col("username").as("username"))
    commentsData
  }
}
