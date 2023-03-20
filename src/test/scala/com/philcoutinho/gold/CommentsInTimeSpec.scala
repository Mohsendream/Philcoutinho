package com.philcoutinho.gold

import com.philcoutinho.gold.CommentsInTime.commentsInTime
import com.philcoutinho.silver.Comments.comments
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class timeData(typename: String, created_at: Long, comment_id: String, owner_id: String, comment_username: String, text: String)

class CommentsInTimeSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()

  import spark.implicits._

  "MostCommentintime" should "return the comments in a specific period" in {
    Given("the specified table")
    val inputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val CommentData = comments(inputData)
    When("commentsInTime is invoked")
    val result = commentsInTime(CommentData, spark,"2020-01-01" ,"2021-05-16")
    Then("the result should be returned")
    val expectedResult = Seq(timeData("GraphImage", 1619023963, "18209883163069294", "20740995", "sergiroberto", "💪🏼💪🏼")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

