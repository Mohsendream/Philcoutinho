package com.philcoutinho.silver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Profil {

  def profil(InputData: DataFrame): DataFrame = {
    val profilData = InputData.select(col("GraphProfileInfo.*"))
    profilData
  }
}
