package com.philcoutinho.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object MostActiveInfluencer {

  def mostActiveInfluencer(influencerData: DataFrame, spark: SparkSession): DataFrame = {

    influencerData.createOrReplaceTempView("influencerDataTable")
    val result = spark.sql("SELECT username, date_format(DATE_TRUNC('week', taken_at_timestamp),'yyyy-MM-dd HH:mm:ss') AS week_start_date, COUNT(DISTINCT id) AS NBR FROM influencerDataTable" +
      " GROUP BY username, week_start_date ORDER BY NBR DESC")
    result
  }
}
