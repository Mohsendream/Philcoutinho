package com.philcoutinho

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Profil {
  def profil(df:DataFrame):DataFrame ={
    val df_1=df.select(col("GraphProfileInfo.*"))
    df_1
  }

}
