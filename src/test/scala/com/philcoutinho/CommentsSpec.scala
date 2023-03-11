package com.philcoutinho

import com.philcoutinho.Comments.comments
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Data(created_at: Long, comment_id: String, text: String, owner_id: String, comment_username: String, username: String)

class CommentsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()

  import spark.implicits._

  "Comments" should "Flatten the JSON Dataframe" in {
    Given("the JSON Dataframe")
    val df = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    When("Comments is invoked")
    val result = comments(df)
    Then("the Flattened DataFrame should be returned")
    val expectedResult = Seq(Data(1619023963L, "18209883163069294", "💪🏼💪🏼", "20740995", "sergiroberto", "phil.coutinho")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
