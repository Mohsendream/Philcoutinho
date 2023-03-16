package com.philcoutinho.gold

import com.philcoutinho.gold.MediaPreviewLike.mediapreviewlike
import com.philcoutinho.silver.Posts.posts
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class MediaPreviewData(__typename: String, count: Long)

class MediaPreviewLikeSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  "MediaPreviewLike" should "return the sum of media preview like by typename" in {
    Given("the specified Data")
    val InputData = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    val EdgeMediaPreview = posts(InputData)
    When("mediapreviewlike is invoked")
    val result = mediapreviewlike(EdgeMediaPreview, spark)
    Then("the result should be returned")
    val expectedResult = Seq(MediaPreviewData("GraphImage", 483475L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
