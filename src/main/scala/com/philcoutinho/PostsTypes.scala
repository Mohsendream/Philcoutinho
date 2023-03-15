package com.philcoutinho

import org.apache.spark.sql.{SparkSession, DataFrame}
import com.philcoutinho.Posts.posts
import com.philcoutinho.Comments.comments

object PostsTypes {

  def poststypes(df: DataFrame): DataFrame = {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("comment_Test")
      .getOrCreate()
    val posttable = posts(df)
    val commenttable = comments(df)
    posttable.createOrReplaceTempView("my_posttable")
    commenttable.createOrReplaceTempView("my_commenttable")
    val result = spark.sql("SELECT my_posttable.__typename as post_typename, COUNT(my_commenttable.comment_id) as nbrofcomments, COUNT( DISTINCT my_commenttable.owner_id) as nbrofcommentator " +
      "FROM my_posttable JOIN my_commenttable ON my_posttable.__typename = my_commenttable.__typename GROUP BY post_typename ")

    result
  }
}
