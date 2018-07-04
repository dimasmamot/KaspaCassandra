package me.mamotis.spark.jobs

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{col, from_unixtime, lit, to_utc_timestamp, window}
import org.joda.time.DateTime
import org.apache.spark.sql.types.StringType

object CassandraSignatureStreamJob extends CassandraUtils {

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

    //+++++++++++++Push Signature Hit Company per Second++++++++++++++++++++++
    //++++++++Second
    val signatureHitCompanySecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"alert_msg",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val signatureHitCompanySecDf_2 = signatureHitCompanySecDf_1.select($"company", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitCompanyObjSec(
          company, alert_msg, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsSignatureHitCompanyObjSec: _*)

    val signatureHitCompanySecDs = signatureHitCompanySecDf_2.select($"company", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.SignatureHitCompanyObjSec]

    //++++++++Minute
    val signatureHitCompanyMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"alert_msg",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val signatureHitCompanyMinDf_2 = signatureHitCompanyMinDf_1.select($"company", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitCompanyObjMin(
          company, alert_msg, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsSignatureHitCompanyObjMin: _*)

    val signatureHitCompanyMinDs = signatureHitCompanyMinDf_2.select($"company", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.SignatureHitCompanyObjMin]

    //++++++++Hour
    val signatureHitCompanyHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"alert_msg",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val signatureHitCompanyHourDf_2 = signatureHitCompanyHourDf_1.select($"company", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitCompanyObjHour(
          company, alert_msg, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsSignatureHitCompanyObjHour: _*)

    val signatureHitCompanyHourDs = signatureHitCompanyHourDf_2.select($"company", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.SignatureHitCompanyObjHour]

    //++++++++Day
    val signatureHitCompanyDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"alert_msg",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val signatureHitCompanyDayDf_2 = signatureHitCompanyDayDf_1.select($"company", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitCompanyObjDay(
          company, alert_msg, year, month, day, value
        )
    }.toDF(ColsArtifact.colsSignatureHitCompanyObjDay: _*)

    val signatureHitCompanyDayDs = signatureHitCompanyDayDf_2.select($"company", $"alert_msg", $"year",
      $"month", $"day", $"value").as[Commons.SignatureHitCompanyObjDay]

    //+++++++++++++Push Signature Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second
    val signatureHitDeviceIdSecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"alert_msg",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val signatureHitDeviceIdSecDf_2 = signatureHitDeviceIdSecDf_1.select($"device_id", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitDeviceIdObjSec(
          device_id, alert_msg, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsSignatureHitDeviceIdObjSec: _*)

    val signatureHitDeviceIdSecDs = signatureHitDeviceIdSecDf_2.select($"device_id", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.SignatureHitDeviceIdObjSec]

    //++++++++Minute
    val signatureHitDeviceIdMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"alert_msg",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val signatureHitDeviceIdMinDf_2 = signatureHitDeviceIdMinDf_1.select($"device_id", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitDeviceIdObjMin(
          device_id, alert_msg, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsSignatureHitDeviceIdObjMin: _*)

    val signatureHitDeviceIdMinDs = signatureHitDeviceIdMinDf_2.select($"device_id", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.SignatureHitDeviceIdObjMin]

    //++++++++Hour
    val signatureHitDeviceIdHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"alert_msg",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val signatureHitDeviceIdHourDf_2 = signatureHitDeviceIdHourDf_1.select($"device_id", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitDeviceIdObjHour(
          device_id, alert_msg, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsSignatureHitDeviceIdObjHour: _*)

    val signatureHitDeviceIdHourDs = signatureHitDeviceIdHourDf_2.select($"device_id", $"alert_msg", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.SignatureHitDeviceIdObjHour]

    //++++++++Day
    val signatureHitDeviceIdDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"alert_msg").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"alert_msg",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val signatureHitDeviceIdDayDf_2 = signatureHitDeviceIdDayDf_1.select($"device_id", $"alert_msg", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val alert_msg = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.SignatureHitDeviceIdObjDay(
          device_id, alert_msg, year, month, day, value
        )
    }.toDF(ColsArtifact.colsSignatureHitDeviceIdObjDay: _*)

    val signatureHitDeviceIdDayDs = signatureHitDeviceIdDayDf_2.select($"device_id", $"alert_msg", $"year",
      $"month", $"day", $"value").as[Commons.SignatureHitDeviceIdObjDay]

    //----------------------------------------------------------------------Writer Definition-------------------------------------------------

    val writerSignatureHitCompanySec = new ForeachWriter[Commons.SignatureHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitCompanyObjSec): Unit = {
        PushArtifact.pushSignatureHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitCompanyMin = new ForeachWriter[Commons.SignatureHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitCompanyObjMin): Unit = {
        PushArtifact.pushSignatureHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitCompanyHour = new ForeachWriter[Commons.SignatureHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitCompanyObjHour): Unit = {
        PushArtifact.pushSignatureHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitCompanyDay = new ForeachWriter[Commons.SignatureHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitCompanyObjDay): Unit = {
        PushArtifact.pushSignatureHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitDeviceIdSec = new ForeachWriter[Commons.SignatureHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitDeviceIdObjSec): Unit = {
        PushArtifact.pushSignatureHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitDeviceIdMin = new ForeachWriter[Commons.SignatureHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitDeviceIdObjMin): Unit = {
        PushArtifact.pushSignatureHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitDeviceIdHour = new ForeachWriter[Commons.SignatureHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitDeviceIdObjHour): Unit = {
        PushArtifact.pushSignatureHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerSignatureHitDeviceIdDay = new ForeachWriter[Commons.SignatureHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.SignatureHitDeviceIdObjDay): Unit = {
        PushArtifact.pushSignatureHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    //----------------------------------------------------------------------Write Stream Query-------------------------------------------------

    val signatureHitCompanySecQuery = signatureHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitCompanyPerSec")
      .foreach(writerSignatureHitCompanySec)
      .start()

    val signatureHitCompanyMinQuery = signatureHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitCompanyPerMin")
      .foreach(writerSignatureHitCompanyMin)
      .start()

    val signatureHitCompanyHourQuery = signatureHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitCompanyPerHour")
      .foreach(writerSignatureHitCompanyHour)
      .start()

    val signatureHitCompanyDayQuery = signatureHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitCompanyPerDay")
      .foreach(writerSignatureHitCompanyDay)
      .start()

    val signatureHitDeviceIdSecQuery = signatureHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitDeviceIdPerSec")
      .foreach(writerSignatureHitDeviceIdSec)
      .start()

    val signatureHitDeviceIdMinQuery = signatureHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitDeviceIdPerMin")
      .foreach(writerSignatureHitDeviceIdMin)
      .start()

    val signatureHitDeviceIdHourQuery = signatureHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitDeviceIdPerHour")
      .foreach(writerSignatureHitDeviceIdHour)
      .start()

    val signatureHitDeviceIdDayQuery = signatureHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("SignatureHitDeviceIdPerDay")
      .foreach(writerSignatureHitDeviceIdDay)
      .start()

    signatureHitCompanySecQuery.awaitTermination()
    signatureHitCompanyMinQuery.awaitTermination()
    signatureHitCompanyHourQuery.awaitTermination()
    signatureHitCompanyDayQuery.awaitTermination()
    signatureHitDeviceIdSecQuery.awaitTermination()
    signatureHitDeviceIdMinQuery.awaitTermination()
    signatureHitDeviceIdHourQuery.awaitTermination()
    signatureHitDeviceIdDayQuery.awaitTermination()
  }

}
