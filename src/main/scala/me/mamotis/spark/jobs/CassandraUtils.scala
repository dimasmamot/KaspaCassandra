package me.mamotis.spark.jobs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector

private[jobs] trait CassandraUtils {
  def getSparkContext(session: SparkSession): SparkContext = {
    session.sparkContext
  }

  def getSparkSession(strings: Array[String]): SparkSession = {
    val conf = new SparkConf(true)
      .setMaster("local[2]")
      .setAppName("Cassandra Push & Aggregate")
      .set("spark.app.id", "CassandraDataStream")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    session
  }

  def getCassandraSession(context: SparkContext): CassandraConnector = {
    val cassandraSession = CassandraConnector.apply(context.getConf)

    cassandraSession
  }
}
