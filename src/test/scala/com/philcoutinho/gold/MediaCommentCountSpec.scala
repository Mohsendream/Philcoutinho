package com.philcoutinho.gold

import com.philcoutinho.gold.MediaCommentCount.mediaCommentCount
import com.philcoutinho.silver.Posts.posts
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class mediaCommentCountData(typename: String, count: Long)

class MediaCommentCountSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test")
    .getOrCreate()

  import spark.implicits._

  "MediaCommentCount" should "return the sum of media cimment by typename" in {
    Given("the specified Data")
    val inputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val edgeMediaComment = posts(inputData)
    When("MediaCommentCount is invoked")
    val result = mediaCommentCount(edgeMediaComment, spark)
    Then("the result should be returned")
    val expectedResult = Seq(mediaCommentCountData("GraphImage", 80L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}