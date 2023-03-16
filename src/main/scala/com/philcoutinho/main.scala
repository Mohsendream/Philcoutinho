package com.philcoutinho

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.philcoutinho.gold.CommentsInTime.commentsintime
import com.philcoutinho.gold.MediaPreviewLike.mediapreviewlike
import com.philcoutinho.gold.MediaCommentCount.mediacommentcount
import com.philcoutinho.gold.MostCommentor.mostcommentor
import com.philcoutinho.gold.PostTypes.posttypes
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
    val outpath = "C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\"
    val InputData = spark.read.option("multiLine", true).json(path)
    val CommentsData = comments(InputData)
    CommentsData.write.parquet(outpath + "CommentsTable" + ".parquet")
    val PostsData = posts(InputData)
    PostsData.write.parquet(outpath + "PostsTable" + ".parquet")
    val ProfilData = profil(InputData)
    ProfilData.write.parquet(outpath + "ProfilTable" + ".parquet")
    val TimeCommentData = commentsintime(CommentsData, spark)
    TimeCommentData.write.parquet(outpath + "TimeCommentTable" + ".parquet")
    val MediaCommentCountData = mediacommentcount(PostsData, spark)
    MediaCommentCountData.write.parquet(outpath + "MediaCommentCountTable" + ".parquet")
    val MediaPreviewLikeData = mediapreviewlike(PostsData, spark)
    MediaPreviewLikeData.write.parquet(outpath + "MediaPreviewLikeTable" + ".parquet")
    val MostCommentorData = mostcommentor(CommentsData, spark)
    MostCommentorData.write.parquet(outpath + "MostCommentorTable" + ".parquet")
    val PostTypesData = posttypes(PostsData, CommentsData, spark)
    PostTypesData.write.parquet(outpath + "PostTypesTable" + ".parquet")

  }
}
