package com.philcoutinho
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, _}

object Comments {
  def Comments(df:DataFrame): DataFrame= {
    val df_1 = df.select(explode(col("GraphImages")).as("GraphImage"))
    val df_2 = df_1.select(col("GraphImage.*"))
    val df_3 = df_2.select(explode(col("comments.data")).as("Data"),
      col("username").as("username"))
    val df_4=df_3.select(col("data.created_at").as("created_at"),
      col("data.id").as("comment_id"),
      col("data.text").as("text"),
      col("Data.owner.id").as("owner_id"),
      col("Data.owner.username").as("comment_username"),
     col("username").as("username")
    )
    df_4
  }
}

