package com.philcoutinho

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.philcoutinho.gold.CommentsInTime.commentsInTime
import com.philcoutinho.gold.MediaPreviewLike.mediaPreviewLike
import com.philcoutinho.gold.MediaCommentCount.mediaCommentCount
import com.philcoutinho.gold.MostCommentor.mostCommentor
import com.philcoutinho.gold.PostTypes.postTypes
import com.philcoutinho.silver.Posts.posts
import com.philcoutinho.silver.Profil.profil
import com.philcoutinho.silver.Comments.comments

object main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("main")
      .getOrCreate()
    val path = "C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json"
    val inputData = spark.read.option("multiLine", true).json(path)
    val commentsData = comments(inputData)
    commentsData.write.parquet( "CommentsTable.parquet")
    val postsData = posts(inputData)
    postsData.write.parquet( "PostsTable.parquet")
    val profilData = profil(inputData)
    profilData.write.parquet( "ProfilTable.parquet")
    val timeCommentData = commentsInTime(commentsData, spark)
    timeCommentData.write.parquet("TimeCommentTable.parquet")
    val mediaCommentCountData = mediaCommentCount(postsData, spark)
    mediaCommentCountData.write.parquet("MediaCommentCountTable.parquet")
    val mediaPreviewLikeData = mediaPreviewLike(postsData, spark)
    mediaPreviewLikeData.write.parquet("MediaPreviewLikeTable.parquet")
    val mostCommentorData = mostCommentor(commentsData, spark)
    mostCommentorData.write.parquet("MostCommentorTable.parquet")
    val PostTypesData = postTypes(postsData, commentsData, spark)
    PostTypesData.write.parquet("PostTypesTable.parquet")

  }
}
