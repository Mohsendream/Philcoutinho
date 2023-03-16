package com.philcoutinho.silver

import com.philcoutinho.silver.Profil.profil
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class Info(biography: String, followers_count: Long, following_count: Long, full_name: String, id: String, is_business_account: Boolean,
                is_joined_recently: Boolean, is_private: Boolean, posts_count: Long)

case class profil_data(created_time: Long, info: Info, username: String)

class ProfilSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()

  import spark.implicits._

  "Profil" should "Flatten the JSON Dataframe" in {
    Given("the JSON Dataframe")
    val df = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    When("Profil is invoked")
    val result = profil(df)
    Then("the Flattened DataFrame should be returned")
    val expectedResult = Seq(profil_data(1286323200L, Info("", 23156762L, 1092L, "Philippe Coutinho", "1382894360", false, false, false, 618L), "phil.coutinho")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}