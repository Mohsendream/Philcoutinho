package com.philcoutinho

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Profil {

  def profil(df: DataFrame): DataFrame = {
    val profilData = df.select(col("GraphProfileInfo.*"))
    profilData
  }
}
