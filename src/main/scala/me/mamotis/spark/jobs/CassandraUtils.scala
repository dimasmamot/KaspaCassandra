package me.mamotis.spark.jobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

private[jobs] trait CassandraUtils {
  def getSparkContext(args: Array[String]): SparkContext = {
    getSparkSession(args).sparkContext
  }

  def getSparkSession(strings: Array[String]): SparkSession = {
    val conf = new SparkConf(true)
      .setMaster("local[4]")
      .setAppName("Cassandra Push & Aggregate")
      .set("spark.app.id", "CassandraDataStream")
      .set("spark.cassandra.connection.host", "10.252.108.99")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    session
  }
}
