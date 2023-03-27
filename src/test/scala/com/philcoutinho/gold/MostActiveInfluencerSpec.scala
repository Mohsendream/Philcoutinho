package com.philcoutinho.gold

import com.philcoutinho.gold.MostActiveInfluencer.mostActiveInfluencer
import com.philcoutinho.silver.Posts.posts
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class activeInfluencerData(influencer: String, week_start_date: String, nbrOfPostsPerWeek: Long)

class MostActiveInfluencerSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  "MostActiveInfluence" should "return the most active influencer based on the number of posts per week" in {
    Given("the specified table")
    val inputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val influencerData = posts(inputData)
    When("commentsInTime is invoked")
    val result = mostActiveInfluencer(influencerData, spark)
    Then("the result should be returned")
    val expectedResult = Seq(activeInfluencerData("phil.coutinho", "2021-04-19 00:00:00", 1L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
