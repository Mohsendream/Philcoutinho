package com.philcoutinho

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
object Posts {
  def Posts(df : DataFrame): DataFrame={
    val df_1=df.select(explode(col("GraphImages")).as("GraphImage"))
    val df_2 = df_1.select(col("GraphImage.*")).drop("comments", "display_url","gating_info","location",
      "media_preview", "thumbnail_resources", "thumbnail_src", "urls")
    df_2
  }
}
