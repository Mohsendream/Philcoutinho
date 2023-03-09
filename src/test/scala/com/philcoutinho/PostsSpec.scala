package com.philcoutinho

import com.philcoutinho.Posts.Posts
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class Owner(id: String)
case class CommentOwner(id: String, profile_pic_url: String, username: String)
case class Dimensions(height: Long, width: Long)
case class EdgeMediaPreviewLike(count: Long)
case class EdgeMediaToCaption(edges: Edges)
case class Edges(node:List[Node])
case class Node(text: String)
case class EdgeMediaToComment(count: Long)
case class posts_data  (__typename : String, comments_disabled : Boolean, dimensions : Dimensions, edge_media_preview_like: EdgeMediaPreviewLike,
                        edge_media_to_caption: EdgeMediaToCaption, edge_media_to_comment: EdgeMediaToComment,  id : String, is_video: Boolean,
                         owner: Owner, shortcode: String, tags: List[String], taken_at_timestamp: Long, username: String)

class PostsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark= SparkSession
    .builder()
    .master("local[*]")
    .appName("comment_Test")
    .getOrCreate()
  import spark.implicits._
  "Profil" should "Flatten the JSON Dataframe" in {
    Given("the JSON Dataframe")
    val df = spark.read.option("multiLine", true).json("C:\\Users\\user\\IdeaProjects\\PhilCoutinho_2\\target\\sample.json")
    When("Profil is invoked")
    val posts = Posts(df)
    Then("the Flattened DataFrame should be returned")
    val expectedResult =Seq(posts_data("GraphImage", false, Dimensions(720L, 1080L), EdgeMediaPreviewLike(483475L), EdgeMediaToCaption(Edges(List(Node("Cada dia é uma nova batalha, que exige o meu máximo! \nA recuperação é lenta, requer paciência e dedicação. \nOs desafios sempre me motivaram. Estou trabalhando firme e estou convicto que voltarei melhor e mais forte a fazer o que mais amo.\nDEUS está comigo e tenho certeza que os que gostam de mim e do meu trabalho também! \nObrigado por toda positividade transmitida.💪🙏\n\n‘’Tudo tem o seu tempo determinado, e há tempo para todo o propósito debaixo do céu.”\n\nEclesiastes 3:1\n\n#borapracima #gratidaoaDEUS\n#embrevetamodevolta #féemDEUS #focadoemotivado")))),
      EdgeMediaToComment(80L),"2556864304565671217" ,false, Owner("1382894360"), "CN7zonEg1Ux", List( "embrevetamodevolta", "gratidaoaDEUS", "focadoemotivado", "borapracima", "féemDEUS"), 1619021998L, "phil.coutinho"  )).toDF()
    expectedResult.as[posts_data].collect() should contain theSameElementsAs posts.as[posts_data].collect()
  }
}