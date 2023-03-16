package com.philcoutinho.gold

import com.philcoutinho.gold.CommentsInTime.commentsintime
import com.philcoutinho.silver.Comments.comments
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class TimeData (__typename: String, created_at: Long, comment_id: String, owner_id: String, comment_username: String, text : String)

class CommentsInTimeSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()

  import spark.implicits._

  "MostCommentintime" should "return the comments in a specific period" in {
    Given("the specified table")
    val InputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val CommentData = comments(InputData)
    When("commentsInTime is invoked")
    val result = commentsintime(CommentData,spark)
    Then("the result should be returned")
    val expectedResult = Seq(TimeData("GraphImage", 1619023963, "18209883163069294", "20740995", "sergiroberto","💪🏼💪🏼")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

