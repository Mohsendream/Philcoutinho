package com.philcoutinho.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Profil {

  def profil(InputData: DataFrame): DataFrame = {

    val profilData = InputData.select(col("GraphProfileInfo.created_time").as("created_at"),
      col("GraphProfileInfo.info.biography").as("Biography"),
      col("GraphProfileInfo.info.followers_count").as("followers_count"),
      col("GraphProfileInfo.info.following_count").as("following_count"),
      col("GraphProfileInfo.info.full_name").as("full_name"),
      col("GraphProfileInfo.info.id").as("id"),
      col("GraphProfileInfo.info.is_business_account").as("is_business_account"),
      col("GraphProfileInfo.info.is_joined_recently").as("is_joined_recently"),
      col("GraphProfileInfo.info.is_private").as("is_private"),
      col("GraphProfileInfo.info.posts_count").as("posts_count"),
      col("GraphProfileInfo.username").as("username"))

    profilData
  }
}
