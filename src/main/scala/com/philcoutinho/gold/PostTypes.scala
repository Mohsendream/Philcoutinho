package com.philcoutinho.gold

import org.apache.spark.sql.{SparkSession, DataFrame}

object PostTypes {

  def posttypes(PostData: DataFrame, CommentData: DataFrame, spark: SparkSession): DataFrame = {
    PostData.createOrReplaceTempView("my_posttable")
    CommentData.createOrReplaceTempView("my_commenttable")
    val result = spark.sql("SELECT my_posttable.__typename as post_typename, COUNT(my_commenttable.comment_id) as nbrofcomments, COUNT( DISTINCT my_commenttable.owner_id) as nbrofcommentator " +
      "FROM my_posttable JOIN my_commenttable ON my_posttable.__typename = my_commenttable.__typename GROUP BY post_typename ")
    result
  }
}
