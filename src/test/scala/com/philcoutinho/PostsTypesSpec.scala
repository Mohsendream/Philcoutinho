package com.philcoutinho

import com.philcoutinho.PostsTypes.poststypes
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class types_Data(__typename : String, nbrofcomments : Long, nbrofcommentator : Long)
class PostsTypesSpec  extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("TEST")
    .getOrCreate()
  import spark.implicits._

  "PostsTypes" should "return table grouped by typename with nbr of comments and nbr of people who commented each type" in {
    Given("the specified table")
    val df = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    When ("PostsTypes is invoked")
    val result = poststypes(df)
    result.show()
    Then("the result should be returned")
    val expectedResult=Seq(types_Data("GraphImage", 1L, 1L),types_Data("GraphSidecar", 33L, 31L)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

}
