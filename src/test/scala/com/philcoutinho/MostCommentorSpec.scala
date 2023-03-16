package com.philcoutinho

import com.philcoutinho.MostCommentor.mostcommentor
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Comments_Data(owner_id: String,  comment_username: String, sum: Long)

class MostCommentorSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()
  import spark.implicits._

  "MostCommentor" should "return the person who commented the most" in {
    Given("the specified table")
    val df = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    When ("mostcommentor is invoked")
    val result = mostcommentor(df)
    Then("the result should be returned")
    val expectedResult=Seq(Comments_Data("20740995", "sergiroberto", 1L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}






