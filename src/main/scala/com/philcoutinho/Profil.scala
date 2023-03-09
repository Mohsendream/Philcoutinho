package com.philcoutinho

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Profil {
  def Profil(df:DataFrame):DataFrame ={
    val df_1=df.select(col("GraphProfileInfo.*")).drop(col("GraphProfileInfo.info.profile_pic_url"))
    df_1
  }

}
