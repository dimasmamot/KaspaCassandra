package me.mamotis.spark.jobs

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import org.apache.spark.sql.{ForeachWriter}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, to_utc_timestamp, window}
import org.joda.time.DateTime

import org.apache.spark.sql.types.StringType

object CassandraEventStreamJob extends CassandraUtils {

  def main(args: Array[String]): Unit = {
    //-----------------------------------------------------------------Kafka Parameter------------------------------------------------------

    val kafkaUrl = "127.0.0.1:9092"
    val schemaRegistryURL = "http://127.0.0.1:8081"
    val topic = "snoqttv5"

    //------------------------------------------------------------------Avro Decription Key------------------------------------------------

    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
    val keyDes = utils.deserializerForSubject(topic + "-key")
    val valDes = utils.deserializerForSubject(topic + "-value")
    val spark = getSparkSession(args)
    val connector = getCassandraSession(getSparkContext(spark))

    // set implicit and log level
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    //----------------------------------------------------------------------Kafka Definition-------------------------------------------------

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

    //----------------------------------------------------------------------Dataframe Parsing------------------------------------------------

    //+++++++++++++Push Raw Data Event++++++++++++++++++++++
    val eventDf = parsedRawDf.select(
      $"timestamp", $"device_id", $"protocol", $"ip_type", $"src_mac", $"dest_mac", $"src_ip",
      $"dest_ip", $"src_port", $"dst_port", $"alert_msg", $"classification", $"priority", $"sig_id",
      $"sig_gen", $"sig_rev", $"company"
    ).map { r =>
      val device_id = r.getAs[String](1)
      val protocol = r.getAs[String](2)
      val ip_type = r.getAs[String](3)
      val src_mac = r.getAs[String](4)
      val dest_mac = r.getAs[String](5)
      val src_ip = r.getAs[String](6)
      val dest_ip = r.getAs[String](7)
      val src_port = r.getAs[Long](8).toInt
      val dst_port = r.getAs[Long](9).toInt
      val alert_msg = r.getAs[String](10)
      val classification = r.getAs[Long](11).toInt
      val priority = r.getAs[Long](12).toInt
      val sig_id = r.getAs[Long](13).toInt
      val sig_gen = r.getAs[Long](14).toInt
      val sig_rev = r.getAs[Long](15).toInt
      val company = r.getAs[String](16)
      val src_country = "placeholder"

      val date = new DateTime((r.getAs[String](0).toDouble * 1000).toLong)
      val year = date.getYear()
      val month = date.getMonthOfYear()
      val day = date.getDayOfMonth()
      val hour = date.getHourOfDay()
      val minute = date.getMinuteOfHour()
      val second = date.getSecondOfMinute()

      new Commons.EventObj(
        company, device_id, year, month, day, hour, minute, second,
        protocol, ip_type, src_mac, dest_mac, src_ip, dest_ip,
        src_port, dst_port, alert_msg, classification, priority,
        sig_id, sig_gen, sig_rev, src_country
      )
    }.toDF(ColsArtifact.colsEventObj: _*)

    val eventDs = eventDf.select($"company", $"device_id", $"year", $"month",
      $"day", $"hour", $"minute", $"second", $"protocol", $"ip_type",
      $"src_mac", $"dest_mac", $"src_ip", $"dest_ip", $"src_port",
      $"dst_port", $"alert_msg", $"classification", $"priority",
      $"sig_id", $"sig_gen", $"sig_rev", $"src_country").as[Commons.EventObj]


    //+++++++++++++Push Event Hit Company per Second++++++++++++++++++++++
    //+++++Second
    val eventHitCompanySecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company").withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val eventHitCompanySecDf_2 = eventHitCompanySecDf_1.select($"company", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](2)

        new Commons.EventHitCompanyObjSec(
          company, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjSec: _*)

    val eventHitCompanySecDs = eventHitCompanySecDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitCompanyObjSec]

    //+++++Minute
    val eventHitCompanyMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company").withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val eventHitCompanyMinDf_2 = eventHitCompanyMinDf_1.select($"company", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](2)

        new Commons.EventHitCompanyObjMin(
          company, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjMin: _*)

    val eventHitCompanyMinDs = eventHitCompanyMinDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitCompanyObjMin]

    //+++++Hour
    val eventHitCompanyHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company").withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val eventHitCompanyHourDf_2 = eventHitCompanyHourDf_1.select($"company", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](2)

        new Commons.EventHitCompanyObjHour(
          company, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjHour: _*)

    val eventHitCompanyHourDs = eventHitCompanyHourDf_2.select($"company", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.EventHitCompanyObjHour]

    //+++++Day
    val eventHitCompanyDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company").withColumn("value", lit(1)
    ).groupBy(
      $"company",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val eventHitCompanyDayDf_2 = eventHitCompanyDayDf_1.select($"company", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](2)

        new Commons.EventHitCompanyObjDay(
          company, year, month, day, value
        )
    }.toDF(ColsArtifact.colsEventHitCompanyObjDay: _*)

    val eventHitCompanyDayDs = eventHitCompanyDayDf_2.select($"company", $"year",
      $"month", $"day", $"value").as[Commons.EventHitCompanyObjDay]

    //+++++++++++++Push Event Hit DeviceId per Second++++++++++++++++++++++
    //+++++Second
    val eventHitDeviceIdSecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id").withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val eventHitDeviceIdSecDf_2 = eventHitDeviceIdSecDf_1.select($"device_id", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](2)

        new Commons.EventHitDeviceIdObjSec(
          device_id, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsEventHitDeviceIdObjSec: _*)

    val eventHitDeviceIdSecDs = eventHitDeviceIdSecDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.EventHitDeviceIdObjSec]

    //+++++Minute
    val eventHitDeviceIdMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id").withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val eventHitDeviceIdMinDf_2 = eventHitDeviceIdMinDf_1.select($"device_id", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](2)

        new Commons.EventHitDeviceIdObjMin(
          device_id, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsEventHitDeviceIdObjMin: _*)

    val eventHitDeviceIdMinDs = eventHitDeviceIdMinDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.EventHitDeviceIdObjMin]

    //+++++Hour
    val eventHitDeviceIdHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id").withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val eventHitDeviceIdHourDf_2 = eventHitDeviceIdHourDf_1.select($"device_id", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](2)

        new Commons.EventHitDeviceIdObjHour(
          device_id, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsEventHitDeviceIdObjHour: _*)

    val eventHitDeviceIdHourDs = eventHitDeviceIdHourDf_2.select($"device_id", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.EventHitDeviceIdObjHour]

    //+++++Day
    val eventHitDeviceIdDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id").withColumn("value", lit(1)
    ).groupBy(
      $"device_id",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val eventHitDeviceIdDayDf_2 = eventHitDeviceIdDayDf_1.select($"device_id", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)

        val epoch = r.getAs[Timestamp](1).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](2)

        new Commons.EventHitDeviceIdObjDay(
          device_id, year, month, day, value
        )
    }.toDF(ColsArtifact.colsEventHitDeviceIdObjDay: _*)

    val eventHitDeviceIdDayDs = eventHitDeviceIdDayDf_2.select($"device_id", $"year",
      $"month", $"day", $"value").as[Commons.EventHitDeviceIdObjDay]

    //----------------------------------------------------------------------Writer Definition-------------------------------------------------

    val writerEvent = new ForeachWriter[Commons.EventObj] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventObj): Unit = {
        PushArtifact.pushRawData(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanySec = new ForeachWriter[Commons.EventHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjSec): Unit = {
        PushArtifact.pushEventHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyMin = new ForeachWriter[Commons.EventHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjMin): Unit = {
        PushArtifact.pushEventHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyHour = new ForeachWriter[Commons.EventHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjHour): Unit = {
        PushArtifact.pushEventHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitCompanyDay = new ForeachWriter[Commons.EventHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitCompanyObjDay): Unit = {
        PushArtifact.pushEventHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdSec = new ForeachWriter[Commons.EventHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjSec): Unit = {
        PushArtifact.pushEventHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdMin = new ForeachWriter[Commons.EventHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjMin): Unit = {
        PushArtifact.pushEventHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdHour = new ForeachWriter[Commons.EventHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjHour): Unit = {
        PushArtifact.pushEventHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerEventHitDeviceIdDay = new ForeachWriter[Commons.EventHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.EventHitDeviceIdObjDay): Unit = {
        PushArtifact.pushEventHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    //----------------------------------------------------------------------Write Stream Query-------------------------------------------------

    val eventPushQuery = eventDs
      .writeStream
      .outputMode("append")
      .queryName("Event Push to Cassandra")
      .foreach(writerEvent)
      .start()

    val eventHitCompanySecQuery = eventHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerSec")
      .foreach(writerEventHitCompanySec)
      .start()

    val eventHitCompanyMinQuery = eventHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerMin")
      .foreach(writerEventHitCompanyMin)
      .start()

    val eventHitCompanyHourQuery = eventHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerHour")
      .foreach(writerEventHitCompanyHour)
      .start()

    val eventHitCompanyDayQuery = eventHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitCompanyPerDay")
      .foreach(writerEventHitCompanyDay)
      .start()

    val eventHitDeviceIdSecQuery = eventHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerSec")
      .foreach(writerEventHitDeviceIdSec)
      .start()

    val eventHitDeviceIdMinQuery = eventHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerMin")
      .foreach(writerEventHitDeviceIdMin)
      .start()

    val eventHitDeviceIdHourQuery = eventHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerHour")
      .foreach(writerEventHitDeviceIdHour)
      .start()

    val eventHitDeviceIdDayQuery = eventHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("EventHitDeviceIdPerDay")
      .foreach(writerEventHitDeviceIdDay)
      .start()


    eventPushQuery.awaitTermination()

    eventHitCompanySecQuery.awaitTermination()
    eventHitCompanyMinQuery.awaitTermination()
    eventHitCompanyHourQuery.awaitTermination()
    eventHitCompanyDayQuery.awaitTermination()
    eventHitDeviceIdSecQuery.awaitTermination()
    eventHitDeviceIdMinQuery.awaitTermination()
    eventHitDeviceIdHourQuery.awaitTermination()
    eventHitDeviceIdDayQuery.awaitTermination()

    spark.stop()
  }


}
