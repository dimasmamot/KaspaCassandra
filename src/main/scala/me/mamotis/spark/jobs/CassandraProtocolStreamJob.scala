package me.mamotis.spark.jobs

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import me.mamotis.spark.jobs.CassandraSignatureStreamJob.{getCassandraSession, getSparkContext, getSparkSession}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{col, from_unixtime, lit, to_utc_timestamp, window}
import org.joda.time.DateTime
import org.apache.spark.sql.types.StringType

object CassandraProtocolStreamJob {

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

    //+++++++++++++Push Protocol Hit Company per Second++++++++++++++++++++++
    //++++++++Second
    val protocolHitCompanySecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolHitCompanySecDf_2 = protocolHitCompanySecDf_1.select($"company", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitCompanyObjSec(
          company, protocol, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolHitCompanyObjSec: _*)

    val protocolHitCompanySecDs = protocolHitCompanySecDf_2.select($"company", $"protocol", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolHitCompanyObjSec]

    //++++++++Minute
    val protocolHitCompanyMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolHitCompanyMinDf_2 = protocolHitCompanyMinDf_1.select($"company", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitCompanyObjMin(
          company, protocol, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolHitCompanyObjMin: _*)

    val protocolHitCompanyMinDs = protocolHitCompanyMinDf_2.select($"company", $"protocol", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolHitCompanyObjMin]

    //++++++++Hour
    val protocolHitCompanyHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolHitCompanyHourDf_2 = protocolHitCompanyHourDf_1.select($"company", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitCompanyObjHour(
          company, protocol, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolHitCompanyObjHour: _*)

    val protocolHitCompanyHourDs = protocolHitCompanyHourDf_2.select($"company", $"protocol", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolHitCompanyObjHour]

    //++++++++Day
    val protocolHitCompanyDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolHitCompanyDayDf_2 = protocolHitCompanyDayDf_1.select($"company", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitCompanyObjDay(
          company, protocol, year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolHitCompanyObjDay: _*)

    val protocolHitCompanyDayDs = protocolHitCompanyDayDf_2.select($"company", $"protocol", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolHitCompanyObjDay]

    //+++++++++++++Push Protocol Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second
    val protocolHitDeviceIdSecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolHitDeviceIdSecDf_2 = protocolHitDeviceIdSecDf_1.select($"device_id", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitDeviceIdObjSec(
          device_id, protocol, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolHitDeviceIdObjSec: _*)

    val protocolHitDeviceIdSecDs = protocolHitDeviceIdSecDf_2.select($"device_id", $"protocol", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolHitDeviceIdObjSec]

    //++++++++Minute
    val protocolHitDeviceIdMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolHitDeviceIdMinDf_2 = protocolHitDeviceIdMinDf_1.select($"device_id", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitDeviceIdObjMin(
          device_id, protocol, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolHitDeviceIdObjMin: _*)

    val protocolHitDeviceIdMinDs = protocolHitDeviceIdMinDf_2.select($"device_id", $"protocol", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolHitDeviceIdObjMin]

    //++++++++Hour
    val protocolHitDeviceIdHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolHitDeviceIdHourDf_2 = protocolHitDeviceIdHourDf_1.select($"device_id", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitDeviceIdObjHour(
          device_id, protocol, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolHitDeviceIdObjHour: _*)

    val protocolHitDeviceIdHourDs = protocolHitDeviceIdHourDf_2.select($"device_id", $"protocol", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolHitDeviceIdObjHour]

    //++++++++Day
    val protocolHitDeviceIdDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolHitDeviceIdDayDf_2 = protocolHitDeviceIdDayDf_1.select($"device_id", $"protocol", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.ProtocolHitDeviceIdObjDay(
          device_id, protocol, year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolHitDeviceIdObjDay: _*)

    val protocolHitDeviceIdDayDs = protocolHitDeviceIdDayDf_2.select($"device_id", $"protocol", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolHitDeviceIdObjDay]




    //+++++++++++++Push Protocol By SPort Hit Company per Second++++++++++++++++++++++
    //++++++++Second
    val protocolBySPortHitCompanySecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"src_port",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolBySPortHitCompanySecDf_2 = protocolBySPortHitCompanySecDf_1.select($"company", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitCompanyObjSec(
          company, protocol, src_port,  year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitCompanyObjSec: _*)

    val protocolBySPortHitCompanySecDs = protocolBySPortHitCompanySecDf_2.select($"company", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolBySPortHitCompanyObjSec]

    //++++++++Minute
    val protocolBySPortHitCompanyMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"src_port",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolBySPortHitCompanyMinDf_2 = protocolBySPortHitCompanyMinDf_1.select($"company", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitCompanyObjMin(
          company, protocol, src_port,  year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitCompanyObjMin: _*)

    val protocolBySPortHitCompanyMinDs = protocolBySPortHitCompanyMinDf_2.select($"company", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolBySPortHitCompanyObjMin]

    //++++++++Hour
    val protocolBySPortHitCompanyHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"src_port",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolBySPortHitCompanyHourDf_2 = protocolBySPortHitCompanyHourDf_1.select($"company", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitCompanyObjHour(
          company, protocol, src_port,  year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitCompanyObjHour: _*)

    val protocolBySPortHitCompanyHourDs = protocolBySPortHitCompanyHourDf_2.select($"company", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolBySPortHitCompanyObjHour]

    //++++++++Day
    val protocolBySPortHitCompanyDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"src_port",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolBySPortHitCompanyDayDf_2 = protocolBySPortHitCompanyDayDf_1.select($"company", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitCompanyObjDay(
          company, protocol, src_port,  year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitCompanyObjDay: _*)

    val protocolBySPortHitCompanyDayDs = protocolBySPortHitCompanyDayDf_2.select($"company", $"protocol", $"src_port", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolBySPortHitCompanyObjDay]

    //+++++++++++++Push Protocol By SPort Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second
    val protocolBySPortHitDeviceIdSecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"src_port",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolBySPortHitDeviceIdSecDf_2 = protocolBySPortHitDeviceIdSecDf_1.select($"device_id", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitDeviceIdObjSec(
          device_id, protocol, src_port,  year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitDeviceIdObjSec: _*)

    val protocolBySPortHitDeviceIdSecDs = protocolBySPortHitDeviceIdSecDf_2.select($"device_id", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolBySPortHitDeviceIdObjSec]

    //++++++++Minute
    val protocolBySPortHitDeviceIdMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"src_port",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolBySPortHitDeviceIdMinDf_2 = protocolBySPortHitDeviceIdMinDf_1.select($"device_id", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitDeviceIdObjMin(
          device_id, protocol, src_port,  year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitDeviceIdObjMin: _*)

    val protocolBySPortHitDeviceIdMinDs = protocolBySPortHitDeviceIdMinDf_2.select($"device_id", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolBySPortHitDeviceIdObjMin]

    //++++++++Hour
    val protocolBySPortHitDeviceIdHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"src_port",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolBySPortHitDeviceIdHourDf_2 = protocolBySPortHitDeviceIdHourDf_1.select($"device_id", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitDeviceIdObjHour(
          device_id, protocol, src_port,  year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitDeviceIdObjHour: _*)

    val protocolBySPortHitDeviceIdHourDs = protocolBySPortHitDeviceIdHourDf_2.select($"device_id", $"protocol", $"src_port", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolBySPortHitDeviceIdObjHour]

    //++++++++Day
    val protocolBySPortHitDeviceIdDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"src_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"src_port",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolBySPortHitDeviceIdDayDf_2 = protocolBySPortHitDeviceIdDayDf_1.select($"device_id", $"protocol", $"src_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val src_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.ProtocolBySPortHitDeviceIdObjDay(
          device_id, protocol, src_port,  year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolBySPortHitDeviceIdObjDay: _*)

    val protocolBySPortHitDeviceIdDayDs = protocolBySPortHitDeviceIdDayDf_2.select($"device_id", $"protocol", $"src_port", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolBySPortHitDeviceIdObjDay]



    //+++++++++++++Push Protocol By DPort Hit Company per Second++++++++++++++++++++++
    //++++++++Second
    val protocolByDPortHitCompanySecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"dst_port",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolByDPortHitCompanySecDf_2 = protocolByDPortHitCompanySecDf_1.select($"company", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitCompanyObjSec(
          company, protocol, dst_port,  year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitCompanyObjSec: _*)

    val protocolByDPortHitCompanySecDs = protocolByDPortHitCompanySecDf_2.select($"company", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolByDPortHitCompanyObjSec]

    //++++++++Minute
    val protocolByDPortHitCompanyMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"dst_port",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolByDPortHitCompanyMinDf_2 = protocolByDPortHitCompanyMinDf_1.select($"company", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitCompanyObjMin(
          company, protocol, dst_port,  year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitCompanyObjMin: _*)

    val protocolByDPortHitCompanyMinDs = protocolByDPortHitCompanyMinDf_2.select($"company", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolByDPortHitCompanyObjMin]

    //++++++++Hour
    val protocolByDPortHitCompanyHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"dst_port",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolByDPortHitCompanyHourDf_2 = protocolByDPortHitCompanyHourDf_1.select($"company", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitCompanyObjHour(
          company, protocol, dst_port,  year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitCompanyObjHour: _*)

    val protocolByDPortHitCompanyHourDs = protocolByDPortHitCompanyHourDf_2.select($"company", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolByDPortHitCompanyObjHour]

    //++++++++Day
    val protocolByDPortHitCompanyDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"protocol", $"dst_port",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolByDPortHitCompanyDayDf_2 = protocolByDPortHitCompanyDayDf_1.select($"company", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitCompanyObjDay(
          company, protocol, dst_port,  year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitCompanyObjDay: _*)

    val protocolByDPortHitCompanyDayDs = protocolByDPortHitCompanyDayDf_2.select($"company", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolByDPortHitCompanyObjDay]

    //+++++++++++++Push Protocol By DPort Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second
    val protocolByDPortHitDeviceIdSecDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"dst_port",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val protocolByDPortHitDeviceIdSecDf_2 = protocolByDPortHitDeviceIdSecDf_1.select($"device_id", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitDeviceIdObjSec(
          device_id, protocol, dst_port,  year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitDeviceIdObjSec: _*)

    val protocolByDPortHitDeviceIdSecDs = protocolByDPortHitDeviceIdSecDf_2.select($"device_id", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.ProtocolByDPortHitDeviceIdObjSec]

    //++++++++Minute
    val protocolByDPortHitDeviceIdMinDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"dst_port",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val protocolByDPortHitDeviceIdMinDf_2 = protocolByDPortHitDeviceIdMinDf_1.select($"device_id", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitDeviceIdObjMin(
          device_id, protocol, dst_port,  year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitDeviceIdObjMin: _*)

    val protocolByDPortHitDeviceIdMinDs = protocolByDPortHitDeviceIdMinDf_2.select($"device_id", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.ProtocolByDPortHitDeviceIdObjMin]

    //++++++++Hour
    val protocolByDPortHitDeviceIdHourDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"dst_port",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val protocolByDPortHitDeviceIdHourDf_2 = protocolByDPortHitDeviceIdHourDf_1.select($"device_id", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitDeviceIdObjHour(
          device_id, protocol, dst_port,  year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitDeviceIdObjHour: _*)

    val protocolByDPortHitDeviceIdHourDs = protocolByDPortHitDeviceIdHourDf_2.select($"device_id", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.ProtocolByDPortHitDeviceIdObjHour]

    //++++++++Day
    val protocolByDPortHitDeviceIdDayDf_1 = parsedRawDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"device_id", $"protocol", $"dst_port").withColumn("value", lit(1)
    ).groupBy(
      $"device_id", $"protocol", $"dst_port",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val protocolByDPortHitDeviceIdDayDf_2 = protocolByDPortHitDeviceIdDayDf_1.select($"device_id", $"protocol", $"dst_port", $"windows.start" , $"sum(value)").map{
      r =>
        val device_id = r.getAs[String](0)
        val protocol = r.getAs[String](1)
        val dst_port = r.getAs[Long](2).toInt

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.ProtocolByDPortHitDeviceIdObjDay(
          device_id, protocol, dst_port,  year, month, day, value
        )
    }.toDF(ColsArtifact.colsProtocolByDPortHitDeviceIdObjDay: _*)

    val protocolByDPortHitDeviceIdDayDs = protocolByDPortHitDeviceIdDayDf_2.select($"device_id", $"protocol", $"dst_port", $"year",
      $"month", $"day", $"value").as[Commons.ProtocolByDPortHitDeviceIdObjDay]




    //----------------------------------------------------------------------Writer Definition-------------------------------------------------

    val writerProtocolHitCompanySec = new ForeachWriter[Commons.ProtocolHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitCompanyObjSec): Unit = {
        PushArtifact.pushProtocolHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitCompanyMin = new ForeachWriter[Commons.ProtocolHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitCompanyObjMin): Unit = {
        PushArtifact.pushProtocolHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitCompanyHour = new ForeachWriter[Commons.ProtocolHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitCompanyObjHour): Unit = {
        PushArtifact.pushProtocolHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitCompanyDay = new ForeachWriter[Commons.ProtocolHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitCompanyObjDay): Unit = {
        PushArtifact.pushProtocolHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitDeviceIdSec = new ForeachWriter[Commons.ProtocolHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitDeviceIdObjSec): Unit = {
        PushArtifact.pushProtocolHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitDeviceIdMin = new ForeachWriter[Commons.ProtocolHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitDeviceIdObjMin): Unit = {
        PushArtifact.pushProtocolHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitDeviceIdHour = new ForeachWriter[Commons.ProtocolHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitDeviceIdObjHour): Unit = {
        PushArtifact.pushProtocolHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolHitDeviceIdDay = new ForeachWriter[Commons.ProtocolHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolHitDeviceIdObjDay): Unit = {
        PushArtifact.pushProtocolHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitCompanySec = new ForeachWriter[Commons.ProtocolBySPortHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitCompanyObjSec): Unit = {
        PushArtifact.pushProtocolBySPortHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitCompanyMin = new ForeachWriter[Commons.ProtocolBySPortHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitCompanyObjMin): Unit = {
        PushArtifact.pushProtocolBySPortHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitCompanyHour = new ForeachWriter[Commons.ProtocolBySPortHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitCompanyObjHour): Unit = {
        PushArtifact.pushProtocolBySPortHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitCompanyDay = new ForeachWriter[Commons.ProtocolBySPortHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitCompanyObjDay): Unit = {
        PushArtifact.pushProtocolBySPortHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitDeviceIdSec = new ForeachWriter[Commons.ProtocolBySPortHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitDeviceIdObjSec): Unit = {
        PushArtifact.pushProtocolBySPortHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitDeviceIdMin = new ForeachWriter[Commons.ProtocolBySPortHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitDeviceIdObjMin): Unit = {
        PushArtifact.pushProtocolBySPortHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitDeviceIdHour = new ForeachWriter[Commons.ProtocolBySPortHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitDeviceIdObjHour): Unit = {
        PushArtifact.pushProtocolBySPortHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolBySPortHitDeviceIdDay = new ForeachWriter[Commons.ProtocolBySPortHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolBySPortHitDeviceIdObjDay): Unit = {
        PushArtifact.pushProtocolBySPortHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitCompanySec = new ForeachWriter[Commons.ProtocolByDPortHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitCompanyObjSec): Unit = {
        PushArtifact.pushProtocolByDPortHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitCompanyMin = new ForeachWriter[Commons.ProtocolByDPortHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitCompanyObjMin): Unit = {
        PushArtifact.pushProtocolByDPortHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitCompanyHour = new ForeachWriter[Commons.ProtocolByDPortHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitCompanyObjHour): Unit = {
        PushArtifact.pushProtocolByDPortHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitCompanyDay = new ForeachWriter[Commons.ProtocolByDPortHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitCompanyObjDay): Unit = {
        PushArtifact.pushProtocolByDPortHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitDeviceIdSec = new ForeachWriter[Commons.ProtocolByDPortHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitDeviceIdObjSec): Unit = {
        PushArtifact.pushProtocolByDPortHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitDeviceIdMin = new ForeachWriter[Commons.ProtocolByDPortHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitDeviceIdObjMin): Unit = {
        PushArtifact.pushProtocolByDPortHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitDeviceIdHour = new ForeachWriter[Commons.ProtocolByDPortHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitDeviceIdObjHour): Unit = {
        PushArtifact.pushProtocolByDPortHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerProtocolByDPortHitDeviceIdDay = new ForeachWriter[Commons.ProtocolByDPortHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.ProtocolByDPortHitDeviceIdObjDay): Unit = {
        PushArtifact.pushProtocolByDPortHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    //----------------------------------------------------------------------Write Stream Query-------------------------------------------------

    val protocolHitCompanySecQuery = protocolHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitCompanyPerSec")
      .foreach(writerProtocolHitCompanySec)
      .start()

    val protocolHitCompanyMinQuery = protocolHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitCompanyPerMin")
      .foreach(writerProtocolHitCompanyMin)
      .start()

    val protocolHitCompanyHourQuery = protocolHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitCompanyPerHour")
      .foreach(writerProtocolHitCompanyHour)
      .start()

    val protocolHitCompanyDayQuery = protocolHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitCompanyPerDay")
      .foreach(writerProtocolHitCompanyDay)
      .start()

    val protocolHitDeviceIdSecQuery = protocolHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitDeviceIdPerSec")
      .foreach(writerProtocolHitDeviceIdSec)
      .start()

    val protocolHitDeviceIdMinQuery = protocolHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitDeviceIdPerMin")
      .foreach(writerProtocolHitDeviceIdMin)
      .start()

    val protocolHitDeviceIdHourQuery = protocolHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitDeviceIdPerHour")
      .foreach(writerProtocolHitDeviceIdHour)
      .start()

    val protocolHitDeviceIdDayQuery = protocolHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolHitDeviceIdPerDay")
      .foreach(writerProtocolHitDeviceIdDay)
      .start()

    val protocolBySPortHitCompanySecQuery = protocolBySPortHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitCompanyPerSec")
      .foreach(writerProtocolBySPortHitCompanySec)
      .start()

    val protocolBySPortHitCompanyMinQuery = protocolBySPortHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitCompanyPerMin")
      .foreach(writerProtocolBySPortHitCompanyMin)
      .start()

    val protocolBySPortHitCompanyHourQuery = protocolBySPortHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitCompanyPerHour")
      .foreach(writerProtocolBySPortHitCompanyHour)
      .start()

    val protocolBySPortHitCompanyDayQuery = protocolBySPortHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitCompanyPerDay")
      .foreach(writerProtocolBySPortHitCompanyDay)
      .start()

    val protocolBySPortHitDeviceIdSecQuery = protocolBySPortHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitDeviceIdPerSec")
      .foreach(writerProtocolBySPortHitDeviceIdSec)
      .start()

    val protocolBySPortHitDeviceIdMinQuery = protocolBySPortHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitDeviceIdPerMin")
      .foreach(writerProtocolBySPortHitDeviceIdMin)
      .start()

    val protocolBySPortHitDeviceIdHourQuery = protocolBySPortHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitDeviceIdPerHour")
      .foreach(writerProtocolBySPortHitDeviceIdHour)
      .start()

    val protocolBySPortHitDeviceIdDayQuery = protocolBySPortHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolBySPortHitDeviceIdPerDay")
      .foreach(writerProtocolBySPortHitDeviceIdDay)
      .start()

    val protocolByDPortHitCompanySecQuery = protocolByDPortHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitCompanyPerSec")
      .foreach(writerProtocolByDPortHitCompanySec)
      .start()

    val protocolByDPortHitCompanyMinQuery = protocolByDPortHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitCompanyPerMin")
      .foreach(writerProtocolByDPortHitCompanyMin)
      .start()

    val protocolByDPortHitCompanyHourQuery = protocolByDPortHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitCompanyPerHour")
      .foreach(writerProtocolByDPortHitCompanyHour)
      .start()

    val protocolByDPortHitCompanyDayQuery = protocolByDPortHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitCompanyPerDay")
      .foreach(writerProtocolByDPortHitCompanyDay)
      .start()

    val protocolByDPortHitDeviceIdSecQuery = protocolByDPortHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitDeviceIdPerSec")
      .foreach(writerProtocolByDPortHitDeviceIdSec)
      .start()

    val protocolByDPortHitDeviceIdMinQuery = protocolByDPortHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitDeviceIdPerMin")
      .foreach(writerProtocolByDPortHitDeviceIdMin)
      .start()

    val protocolByDPortHitDeviceIdHourQuery = protocolByDPortHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitDeviceIdPerHour")
      .foreach(writerProtocolByDPortHitDeviceIdHour)
      .start()

    val protocolByDPortHitDeviceIdDayQuery = protocolByDPortHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("ProtocolByDPortHitDeviceIdPerDay")
      .foreach(writerProtocolByDPortHitDeviceIdDay)
      .start()

    protocolHitCompanySecQuery.awaitTermination()
    protocolHitCompanyMinQuery.awaitTermination()
    protocolHitCompanyHourQuery.awaitTermination()
    protocolHitCompanyDayQuery.awaitTermination()
    protocolHitDeviceIdSecQuery.awaitTermination()
    protocolHitDeviceIdMinQuery.awaitTermination()
    protocolHitDeviceIdHourQuery.awaitTermination()
    protocolHitDeviceIdDayQuery.awaitTermination()
    protocolBySPortHitCompanySecQuery.awaitTermination()
    protocolBySPortHitCompanyMinQuery.awaitTermination()
    protocolBySPortHitCompanyHourQuery.awaitTermination()
    protocolBySPortHitCompanyDayQuery.awaitTermination()
    protocolBySPortHitDeviceIdSecQuery.awaitTermination()
    protocolBySPortHitDeviceIdMinQuery.awaitTermination()
    protocolBySPortHitDeviceIdHourQuery.awaitTermination()
    protocolBySPortHitDeviceIdDayQuery.awaitTermination()
    protocolByDPortHitCompanySecQuery.awaitTermination()
    protocolByDPortHitCompanyMinQuery.awaitTermination()
    protocolByDPortHitCompanyHourQuery.awaitTermination()
    protocolByDPortHitCompanyDayQuery.awaitTermination()
    protocolByDPortHitDeviceIdSecQuery.awaitTermination()
    protocolByDPortHitDeviceIdMinQuery.awaitTermination()
    protocolByDPortHitDeviceIdHourQuery.awaitTermination()
    protocolByDPortHitDeviceIdDayQuery.awaitTermination()
  }

}
