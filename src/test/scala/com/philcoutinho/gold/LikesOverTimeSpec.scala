package com.philcoutinho.gold

import com.philcoutinho.gold.LikesOverTime.likesOverTime
import com.philcoutinho.silver.Posts.posts
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class likesData(likes: Long, Date: String, postOwner: String)

class LikesOverTimeSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test")
    .getOrCreate()

  import spark.implicits._

  "LikesOverTime" should "return the nbre of likes with dates correspndingly to an owner" in {
    Given("the specified table")
    val inputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val likes = posts(inputData)
    When("commentsInTime is invoked")
    val result = likesOverTime(likes, spark)
    Then("the result should be returned")
    val expectedResult = Seq(likesData(483475L, "2021-04-21 18:19:58", "phil.coutinho")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
