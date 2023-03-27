package com.philcoutinho.gold

import com.philcoutinho.gold.PostTypes.postTypes
import com.philcoutinho.silver.Posts.posts
import com.philcoutinho.silver.Comments.comments
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class typesData(typename: String, nbrofcomments: Long, nbrofcommentator: Long)

class PostsTypesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("TEST")
    .getOrCreate()

  import spark.implicits._

  "PostsTypes" should "return table grouped by typename with nbr of comments and nbr of people who commented each type" in {
    Given("the specified table")
    val inputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val postData = posts(inputData)
    val commentData = comments(inputData)
    When("PostsTypes is invoked")
    val result = postTypes(postData, commentData, spark)
    Then("the result should be returned")
    val expectedResult = Seq(typesData("GraphImage", 1L, 1L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

