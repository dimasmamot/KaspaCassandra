package me.mamotis.spark.jobs

import com.databricks.spark.avro.ConfluentSparkAvroUtils

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.functions.{col}

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.joda.time.DateTime

import java.util.UUID.randomUUID

object CassandraStreamJob extends CassandraUtils {



  def main(args: Array[String]): Unit = {
    // Kafka related parameter
    val kafkaUrl = "cdh.pens.ac.id:9092"
    val schemaRegistryURL = "http://cdh.pens.ac.id:8081"
    val topic = "snoqttv4"

    // Avro Decryption Code
    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")
    val spark = getSparkSession(args)
    val connector = CassandraConnector.apply(spark.sparkContext.getConf)

    val cols = List("device_id", "year", "month", "day", "hour", "minute", "second",
                    "protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip",
                    "src_port", "dst_port", "alert_msg", "classification", "priority",
                    "sig_id", "sig_gen", "sig_rev", "src_country")

    // set implicit and log level
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    def processRow(value: Commons.EventObj) = {
      connector.withSessionDo{
        session =>
          session.execute(Statements.push_raw_event(randomUUID(), value.device_id, value.year, value.month, value.day, value.hour,
            value.minute, value.second, value.protocol, value.ip_type, value.src_mac, value.dest_mac,
            value.src_ip, value.dest_ip, value.src_port.toInt, value.dst_port.toInt, value.alert_msg, value.classification,
            value.priority, value.sig_id, value.sig_gen, value.sig_rev, value.src_country))
      }
    }

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val decoded = kafkaStreamDF.select(
      valDes(col("value")).alias("value")
    )

    val parsedRawDf = decoded.select("value.*")

    val eventDf = parsedRawDf.select(
      $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev"
    ).map { r =>
      val device_id = r.getAs[String](1)
      val protocol = r.getAs[String](2)
      val ip_type = r.getAs[String](3)
      val src_mac = r.getAs[String](4)
      val dest_mac = r.getAs[String](5)
      val src_ip = r.getAs[String](6)
      val dest_ip = r.getAs[String](7)
      val src_port = r.getAs[Long](8)
      val dst_port = r.getAs[Long](9)
      val alert_msg = r.getAs[String](10)
      val classification = r.getAs[Long](11)
      val priority = r.getAs[Long](12)
      val sig_id = r.getAs[Long](13)
      val sig_gen = r.getAs[Long](14)
      val sig_rev = r.getAs[Long](15)
      val src_country = "placeholder"


      val date = new DateTime((r.getAs[String](0).toDouble * 1000).toLong)
      val year = date.getYear()
      val month = date.getMonthOfYear()
      val day = date.getDayOfYear()
      val hour = date.getHourOfDay()
      val minute = date.getMinuteOfHour()
      val second = date.getSecondOfMinute()

      new Commons.EventObj(
        device_id, year, month, day, hour, minute, second,
        protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip,
        src_port, dst_port, alert_msg, classification, priority,
        sig_id, sig_gen, sig_rev, src_country
      )

    }.toDF(cols: _*)

    eventDf.printSchema

    val eventDs = eventDf.select($"device_id", $"year", $"month",
      $"day", $"hour", $"minute", $"second", $"protocol", $"ip_type",
      $"src_mac", $"dest_mac", $"src_ip", $"dest_ip", $"src_port",
      $"dst_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country").as[Commons.EventObj]

    val writer = new ForeachWriter[Commons.EventObj] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventObj): Unit = {
        processRow(value)
//        println(value.device_id)
//        println(value.year)
//        println(value.month )
//        println(value.day)
//        println(value.hour)
//        println(value.minute)
//        println(value.second)
//        println(value.protocol)
//        println(value.ip_type)
//        println(value.src_mac)
//        println(value.dest_mac)
//        println(value.src_ip)
//        println(value.dest_ip)
//        println(value.src_port)
//        println(value.dst_port)
//        println(value.alert_msg)
//        println(value.classification)
//        println(value.priority)
//        println(value.sig_id)
//        println(value.sig_gen)
//        println(value.sig_rev)
//        println(value.src_country)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }


    val eventPushQuery = eventDs
      .writeStream
//      .format("console")
      .outputMode("append")
      .queryName("Event Push to Cassandra")
      .foreach(writer)
      .start()

    eventPushQuery.awaitTermination()
    spark.stop()
  }


}
