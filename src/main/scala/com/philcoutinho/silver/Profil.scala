package com.philcoutinho.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Profil {

  def profil(df: DataFrame): DataFrame = {
    val profilData = df.select(col("GraphProfileInfo.*"))
    profilData
  }
}
