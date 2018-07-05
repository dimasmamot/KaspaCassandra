package me.mamotis.spark.jobs

import java.sql.Timestamp

import com.databricks.spark.avro.ConfluentSparkAvroUtils
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{col, from_unixtime, lit, to_utc_timestamp, window}
import org.joda.time.DateTime
import org.apache.spark.sql.types.StringType

object CassandraIPCountryStreamJob extends CassandraUtils {
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
      val src_country = "RUSIA"
      val dest_country = "INDONESIA"

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
        sig_id, sig_gen, sig_rev, src_country, dest_country
      )
    }.toDF(ColsArtifact.colsEventObj: _*)
    //----------------------------------------------------------------------Dataframe Parsing------------------------------------------------

    //+++++++++++++Push IP Source Hit Company per Second++++++++++++++++++++++
    //++++++++Second

    val ipSourceHitCompanySecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val ipSourceHitCompanySecDf_2 = ipSourceHitCompanySecDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitCompanyObjSec(
          company, src_ip, country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitCompanyObjSec: _*)

    val ipSourceHitCompanySecDs = ipSourceHitCompanySecDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.IPSourceHitCompanyObjSec]

//  +++++++++ Minute
    val ipSourceHitCompanyMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val ipSourceHitCompanyMinDf_2 = ipSourceHitCompanyMinDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitCompanyObjMin(
          company, src_ip, country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitCompanyObjMin: _*)

    val ipSourceHitCompanyMinDs = ipSourceHitCompanyMinDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.IPSourceHitCompanyObjMin]

//    +++++++++ Hour
    val ipSourceHitCompanyHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val ipSourceHitCompanyHourDf_2 = ipSourceHitCompanyHourDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitCompanyObjHour(
          company, src_ip, country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitCompanyObjHour: _*)

    val ipSourceHitCompanyHourDs = ipSourceHitCompanyHourDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.IPSourceHitCompanyObjHour]

//    +++++++++ Day

    val ipSourceHitCompanyDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val ipSourceHitCompanyDayDf_2 = ipSourceHitCompanyDayDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitCompanyObjDay(
          company, src_ip, country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitCompanyObjDay: _*)

    val ipSourceHitCompanyDayDs = ipSourceHitCompanyDayDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"value").as[Commons.IPSourceHitCompanyObjDay]

    //+++++++++++++Push IP Destination Hit Company per Second++++++++++++++++++++++
    //++++++++Second

    val ipDestHitCompanySecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val ipDestHitCompanySecDf_2 = ipDestHitCompanySecDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitCompanyObjSec(
          company, dest_ip, country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsIPDestHitCompanyObjSec: _*)

    val ipDestHitCompanySecDs = ipDestHitCompanySecDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.IPDestHitCompanyObjSec]

    //  +++++++++ Minute
    val ipDestHitCompanyMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val ipDestHitCompanyMinDf_2 = ipDestHitCompanyMinDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitCompanyObjMin(
          company, dest_ip, country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsIPDestHitCompanyObjMin: _*)

    val ipDestHitCompanyMinDs = ipDestHitCompanyMinDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.IPDestHitCompanyObjMin]

    //    +++++++++ Hour
    val ipDestHitCompanyHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val ipDestHitCompanyHourDf_2 = ipDestHitCompanyHourDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitCompanyObjHour(
          company, dest_ip, country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsIPDestHitCompanyObjHour: _*)

    val ipDestHitCompanyHourDs = ipDestHitCompanyHourDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.IPDestHitCompanyObjHour]

    //    +++++++++ Day
    val ipDestHitCompanyDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val ipDestHitCompanyDayDf_2 = ipDestHitCompanyDayDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitCompanyObjDay(
          company, dest_ip, country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsIPDestHitCompanyObjDay: _*)

    val ipDestHitCompanyDayDs = ipDestHitCompanyDayDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"value").as[Commons.IPDestHitCompanyObjDay]

    //+++++++++++++Push Country Source Hit Company per Second++++++++++++++++++++++
    //++++++++Second

    val countrySourceHitCompanySecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val countrySourceHitCompanySecDf_2 = countrySourceHitCompanySecDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitCompanyObjSec(
          company, src_country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitCompanyObjSec: _*)

    val countrySourceHitCompanySecDs = countrySourceHitCompanySecDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.CountrySrcHitCompanyObjSec]

    //  +++++++++ Minute
    val countrySourceHitCompanyMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val countrySourceHitCompanyMinDf_2 = countrySourceHitCompanyMinDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitCompanyObjMin(
          company, src_country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitCompanyObjMin: _*)

    val countrySourceHitCompanyMinDs = countrySourceHitCompanyMinDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.CountrySrcHitCompanyObjMin]

    //    +++++++++ Hour
    val countrySourceHitCompanyHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val countrySourceHitCompanyHourDf_2 = countrySourceHitCompanyHourDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitCompanyObjHour(
          company, src_country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitCompanyObjHour: _*)

    val countrySourceHitCompanyHourDs = countrySourceHitCompanyHourDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.CountrySrcHitCompanyObjHour]

    //    +++++++++ Day

    val countrySourceHitCompanyDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val countrySourceHitCompanyDayDf_2 = countrySourceHitCompanyDayDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitCompanyObjDay(
          company, src_country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitCompanyObjDay: _*)

    val countrySourceHitCompanyDayDs = countrySourceHitCompanyDayDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"value").as[Commons.CountrySrcHitCompanyObjDay]

    //+++++++++++++Push Country Dest Hit Company per Second++++++++++++++++++++++
    //++++++++Second

    val countryDestHitCompanySecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val countryDestHitCompanySecDf_2 = countryDestHitCompanySecDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitCompanyObjSec(
          company, dest_country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitCompanyObjSec: _*)

    val countryDestHitCompanySecDs = countryDestHitCompanySecDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.CountryDestHitCompanyObjSec]

    //  +++++++++ Minute
    val countryDestHitCompanyMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val countryDestHitCompanyMinDf_2 = countryDestHitCompanyMinDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitCompanyObjMin(
          company, dest_country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitCompanyObjMin: _*)

    val countryDestHitCompanyMinDs = countryDestHitCompanyMinDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.CountryDestHitCompanyObjMin]

    //    +++++++++ Hour
    val countryDestHitCompanyHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val countryDestHitCompanyHourDf_2 = countryDestHitCompanyHourDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitCompanyObjHour(
          company, dest_country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitCompanyObjHour: _*)

    val countryDestHitCompanyHourDs = countryDestHitCompanyHourDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.CountryDestHitCompanyObjHour]

    //    +++++++++ Day

    val countryDestHitCompanyDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val countryDestHitCompanyDayDf_2 = countryDestHitCompanyDayDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitCompanyObjDay(
          company, dest_country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitCompanyObjDay: _*)

    val countryDestHitCompanyDayDs = countryDestHitCompanyDayDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"value").as[Commons.CountryDestHitCompanyObjDay]

    //+++++++++++++Push IP Source Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second

    val ipSourceHitDeviceIdSecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val ipSourceHitDeviceIdSecDf_2 = ipSourceHitDeviceIdSecDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitDeviceIdObjSec(
          company, src_ip, country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitDeviceIdObjSec: _*)

    val ipSourceHitDeviceIdSecDs = ipSourceHitDeviceIdSecDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.IPSourceHitDeviceIdObjSec]

    //  +++++++++ Minute
    val ipSourceHitDeviceIdMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val ipSourceHitDeviceIdMinDf_2 = ipSourceHitDeviceIdMinDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitDeviceIdObjMin(
          company, src_ip, country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitDeviceIdObjMin: _*)

    val ipSourceHitDeviceIdMinDs = ipSourceHitDeviceIdMinDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.IPSourceHitDeviceIdObjMin]

    //    +++++++++ Hour
    val ipSourceHitDeviceIdHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val ipSourceHitDeviceIdHourDf_2 = ipSourceHitDeviceIdHourDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitDeviceIdObjHour(
          company, src_ip, country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitDeviceIdObjHour: _*)

    val ipSourceHitDeviceIdHourDs = ipSourceHitDeviceIdHourDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.IPSourceHitDeviceIdObjHour]

    //    +++++++++ Day

    val ipSourceHitDeviceIdDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_ip", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_ip", $"src_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val ipSourceHitDeviceIdDayDf_2 = ipSourceHitDeviceIdDayDf_1.select($"company", $"src_ip", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val src_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.IPSourceHitDeviceIdObjDay(
          company, src_ip, country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsIPSourceHitDeviceIdObjDay: _*)

    val ipSourceHitDeviceIdDayDs = ipSourceHitDeviceIdDayDf_2.select($"company", $"ip_src", $"country", $"year",
      $"month", $"day", $"value").as[Commons.IPSourceHitDeviceIdObjDay]

    //+++++++++++++Push IP Destination Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second

    val ipDestHitDeviceIdSecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val ipDestHitDeviceIdSecDf_2 = ipDestHitDeviceIdSecDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitDeviceIdObjSec(
          company, dest_ip, country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsIPDestHitDeviceIdObjSec: _*)

    val ipDestHitDeviceIdSecDs = ipDestHitDeviceIdSecDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.IPDestHitDeviceIdObjSec]

    //  +++++++++ Minute
    val ipDestHitDeviceIdMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val ipDestHitDeviceIdMinDf_2 = ipDestHitDeviceIdMinDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitDeviceIdObjMin(
          company, dest_ip, country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsIPDestHitDeviceIdObjMin: _*)

    val ipDestHitDeviceIdMinDs = ipDestHitDeviceIdMinDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.IPDestHitDeviceIdObjMin]

    //    +++++++++ Hour
    val ipDestHitDeviceIdHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val ipDestHitDeviceIdHourDf_2 = ipDestHitDeviceIdHourDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitDeviceIdObjHour(
          company, dest_ip, country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsIPDestHitDeviceIdObjHour: _*)

    val ipDestHitDeviceIdHourDs = ipDestHitDeviceIdHourDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.IPDestHitDeviceIdObjHour]

    //    +++++++++ Day
    val ipDestHitDeviceIdDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_ip", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_ip", $"dest_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val ipDestHitDeviceIdDayDf_2 = ipDestHitDeviceIdDayDf_1.select($"company", $"dest_ip", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)
        val dest_ip = r.getAs[String](1)
        val country = r.getAs[String](2)

        val epoch = r.getAs[Timestamp](3).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](4)

        new Commons.IPDestHitDeviceIdObjDay(
          company, dest_ip, country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsIPDestHitDeviceIdObjDay: _*)

    val ipDestHitDeviceIdDayDs = ipDestHitDeviceIdDayDf_2.select($"company", $"ip_dest", $"country", $"year",
      $"month", $"day", $"value").as[Commons.IPDestHitDeviceIdObjDay]

    //+++++++++++++Push Country Source Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second

    val countrySourceHitDeviceIdSecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val countrySourceHitDeviceIdSecDf_2 = countrySourceHitDeviceIdSecDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitDeviceIdObjSec(
          company, src_country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitDeviceIdObjSec: _*)

    val countrySourceHitDeviceIdSecDs = countrySourceHitDeviceIdSecDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.CountrySrcHitDeviceIdObjSec]

    //  +++++++++ Minute
    val countrySourceHitDeviceIdMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val countrySourceHitDeviceIdMinDf_2 = countrySourceHitDeviceIdMinDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitDeviceIdObjMin(
          company, src_country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitDeviceIdObjMin: _*)

    val countrySourceHitDeviceIdMinDs = countrySourceHitDeviceIdMinDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.CountrySrcHitDeviceIdObjMin]

    //    +++++++++ Hour
    val countrySourceHitDeviceIdHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val countrySourceHitDeviceIdHourDf_2 = countrySourceHitDeviceIdHourDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitDeviceIdObjHour(
          company, src_country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitDeviceIdObjHour: _*)

    val countrySourceHitDeviceIdHourDs = countrySourceHitDeviceIdHourDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.CountrySrcHitDeviceIdObjHour]

    //    +++++++++ Day

    val countrySourceHitDeviceIdDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"src_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"src_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val countrySourceHitDeviceIdDayDf_2 = countrySourceHitDeviceIdDayDf_1.select($"company", $"src_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val src_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.CountrySrcHitDeviceIdObjDay(
          company, src_country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsCountrySrcHitDeviceIdObjDay: _*)

    val countrySourceHitDeviceIdDayDs = countrySourceHitDeviceIdDayDf_2.select($"company", $"src_country", $"year",
      $"month", $"day", $"value").as[Commons.CountrySrcHitDeviceIdObjDay]

    //+++++++++++++Push Country Dest Hit DeviceId per Second++++++++++++++++++++++
    //++++++++Second

    val countryDestHitDeviceIdSecDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 seconds").alias("windows")
    ).sum("value")

    val countryDestHitDeviceIdSecDf_2 = countryDestHitDeviceIdSecDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()
        val second = date.getSecondOfMinute()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitDeviceIdObjSec(
          company, dest_country, year, month, day, hour, minute, second, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitDeviceIdObjSec: _*)

    val countryDestHitDeviceIdSecDs = countryDestHitDeviceIdSecDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"minute", $"second", $"value").as[Commons.CountryDestHitDeviceIdObjSec]

    //  +++++++++ Minute
    val countryDestHitDeviceIdMinDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 minutes").alias("windows")
    ).sum("value")

    val countryDestHitDeviceIdMinDf_2 = countryDestHitDeviceIdMinDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()
        val minute = date.getMinuteOfHour()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitDeviceIdObjMin(
          company, dest_country, year, month, day, hour, minute, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitDeviceIdObjMin: _*)

    val countryDestHitDeviceIdMinDs = countryDestHitDeviceIdMinDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"minute", $"value").as[Commons.CountryDestHitDeviceIdObjMin]

    //    +++++++++ Hour
    val countryDestHitDeviceIdHourDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 hours").alias("windows")
    ).sum("value")

    val countryDestHitDeviceIdHourDf_2 = countryDestHitDeviceIdHourDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()
        val hour = date.getHourOfDay()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitDeviceIdObjHour(
          company, dest_country, year, month, day, hour, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitDeviceIdObjHour: _*)

    val countryDestHitDeviceIdHourDs = countryDestHitDeviceIdHourDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"hour", $"value").as[Commons.CountryDestHitDeviceIdObjHour]

    //    +++++++++ Day

    val countryDestHitDeviceIdDayDf_1 = eventDf.select(to_utc_timestamp(
      from_unixtime($"timestamp"), "GMT").alias("timestamp").cast(StringType), $"company", $"dest_country").withColumn("value", lit(1)
    ).groupBy(
      $"company", $"dest_country",
      window($"timestamp", "1 days").alias("windows")
    ).sum("value")

    val countryDestHitDeviceIdDayDf_2 = countryDestHitDeviceIdDayDf_1.select($"company", $"dest_country", $"windows.start" , $"sum(value)").map{
      r =>
        val company = r.getAs[String](0)

        val dest_country = r.getAs[String](1)

        val epoch = r.getAs[Timestamp](2).getTime

        val date = new DateTime(epoch)
        val year = date.getYear()
        val month = date.getMonthOfYear()
        val day = date.getDayOfMonth()

        val value = r.getAs[Long](3)

        new Commons.CountryDestHitDeviceIdObjDay(
          company, dest_country, year, month, day, value
        )
    }.toDF(ColsArtifact.colsCountryDestHitDeviceIdObjDay: _*)

    val countryDestHitDeviceIdDayDs = countryDestHitDeviceIdDayDf_2.select($"company", $"dest_country", $"year",
      $"month", $"day", $"value").as[Commons.CountryDestHitDeviceIdObjDay]

    //    ------------------------------------------------------------------Writer Definition----------------------------------------------------------------------------

    val writerIPSourceHitCompanySec = new ForeachWriter[Commons.IPSourceHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitCompanyObjSec): Unit = {
        PushArtifact.pushIPSourceHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitCompanyMin = new ForeachWriter[Commons.IPSourceHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitCompanyObjMin): Unit = {
        PushArtifact.pushIPSourceHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitCompanyHour = new ForeachWriter[Commons.IPSourceHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitCompanyObjHour): Unit = {
        PushArtifact.pushIPSourceHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitCompanyDay = new ForeachWriter[Commons.IPSourceHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitCompanyObjDay): Unit = {
        PushArtifact.pushIPSourceHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitDeviceIdSec = new ForeachWriter[Commons.IPSourceHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitDeviceIdObjSec): Unit = {
        PushArtifact.pushIPSourceHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitDeviceIdMin = new ForeachWriter[Commons.IPSourceHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitDeviceIdObjMin): Unit = {
        PushArtifact.pushIPSourceHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitDeviceIdHour = new ForeachWriter[Commons.IPSourceHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitDeviceIdObjHour): Unit = {
        PushArtifact.pushIPSourceHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPSourceHitDeviceIdDay = new ForeachWriter[Commons.IPSourceHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPSourceHitDeviceIdObjDay): Unit = {
        PushArtifact.pushIPSourceHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitCompanySec = new ForeachWriter[Commons.IPDestHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitCompanyObjSec): Unit = {
        PushArtifact.pushIPDestHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitCompanyMin = new ForeachWriter[Commons.IPDestHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitCompanyObjMin): Unit = {
        PushArtifact.pushIPDestHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitCompanyHour = new ForeachWriter[Commons.IPDestHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitCompanyObjHour): Unit = {
        PushArtifact.pushIPDestHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitCompanyDay = new ForeachWriter[Commons.IPDestHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitCompanyObjDay): Unit = {
        PushArtifact.pushIPDestHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitDeviceIdSec = new ForeachWriter[Commons.IPDestHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitDeviceIdObjSec): Unit = {
        PushArtifact.pushIPDestHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitDeviceIdMin = new ForeachWriter[Commons.IPDestHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitDeviceIdObjMin): Unit = {
        PushArtifact.pushIPDestHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitDeviceIdHour = new ForeachWriter[Commons.IPDestHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitDeviceIdObjHour): Unit = {
        PushArtifact.pushIPDestHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerIPDestHitDeviceIdDay = new ForeachWriter[Commons.IPDestHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.IPDestHitDeviceIdObjDay): Unit = {
        PushArtifact.pushIPDestHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitCompanySec = new ForeachWriter[Commons.CountrySrcHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitCompanyObjSec): Unit = {
        PushArtifact.pushCountrySrcHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitCompanyMin = new ForeachWriter[Commons.CountrySrcHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitCompanyObjMin): Unit = {
        PushArtifact.pushCountrySrcHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitCompanyHour = new ForeachWriter[Commons.CountrySrcHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitCompanyObjHour): Unit = {
        PushArtifact.pushCountrySrcHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitCompanyDay = new ForeachWriter[Commons.CountrySrcHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitCompanyObjDay): Unit = {
        PushArtifact.pushCountrySrcHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitDeviceIdSec = new ForeachWriter[Commons.CountrySrcHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitDeviceIdObjSec): Unit = {
        PushArtifact.pushCountrySrcHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitDeviceIdMin = new ForeachWriter[Commons.CountrySrcHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitDeviceIdObjMin): Unit = {
        PushArtifact.pushCountrySrcHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitDeviceIdHour = new ForeachWriter[Commons.CountrySrcHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitDeviceIdObjHour): Unit = {
        PushArtifact.pushCountrySrcHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountrySrcHitDeviceIdDay = new ForeachWriter[Commons.CountrySrcHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountrySrcHitDeviceIdObjDay): Unit = {
        PushArtifact.pushCountrySrcHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitCompanySec = new ForeachWriter[Commons.CountryDestHitCompanyObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitCompanyObjSec): Unit = {
        PushArtifact.pushCountryDestHitCompanySec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitCompanyMin = new ForeachWriter[Commons.CountryDestHitCompanyObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitCompanyObjMin): Unit = {
        PushArtifact.pushCountryDestHitCompanyMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitCompanyHour = new ForeachWriter[Commons.CountryDestHitCompanyObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitCompanyObjHour): Unit = {
        PushArtifact.pushCountryDestHitCompanyHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitCompanyDay = new ForeachWriter[Commons.CountryDestHitCompanyObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitCompanyObjDay): Unit = {
        PushArtifact.pushCountryDestHitCompanyDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitDeviceIdSec = new ForeachWriter[Commons.CountryDestHitDeviceIdObjSec] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitDeviceIdObjSec): Unit = {
        PushArtifact.pushCountryDestHitDeviceIdSec(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitDeviceIdMin = new ForeachWriter[Commons.CountryDestHitDeviceIdObjMin] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitDeviceIdObjMin): Unit = {
        PushArtifact.pushCountryDestHitDeviceIdMin(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitDeviceIdHour = new ForeachWriter[Commons.CountryDestHitDeviceIdObjHour] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitDeviceIdObjHour): Unit = {
        PushArtifact.pushCountryDestHitDeviceIdHour(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

    val writerCountryDestHitDeviceIdDay = new ForeachWriter[Commons.CountryDestHitDeviceIdObjDay] {
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(value: Commons.CountryDestHitDeviceIdObjDay): Unit = {
        PushArtifact.pushCountryDestHitDeviceIdDay(value, connector)
      }

      override def close(errorOrNull: Throwable): Unit = {}
    }

//    ------------------------------------------------------------------------------Write Stream Query--------------------------------------------------

    val ipSourceHitCompanySecQuery = ipSourceHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitCompanyPerSec")
      .foreach(writerIPSourceHitCompanySec)
      .start()

    val ipSourceHitCompanyMinQuery = ipSourceHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitCompanyPerMin")
      .foreach(writerIPSourceHitCompanyMin)
      .start()

    val ipSourceHitCompanyHourQuery = ipSourceHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitCompanyPerHour")
      .foreach(writerIPSourceHitCompanyHour)
      .start()

    val ipSourceHitCompanyDayQuery = ipSourceHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitCompanyPerDay")
      .foreach(writerIPSourceHitCompanyDay)
      .start()

    val ipSourceHitDeviceIdSecQuery = ipSourceHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitDeviceIdPerSec")
      .foreach(writerIPSourceHitDeviceIdSec)
      .start()

    val ipSourceHitDeviceIdMinQuery = ipSourceHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitDeviceIdPerMin")
      .foreach(writerIPSourceHitDeviceIdMin)
      .start()

    val ipSourceHitDeviceIdHourQuery = ipSourceHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitDeviceIdPerHour")
      .foreach(writerIPSourceHitDeviceIdHour)
      .start()

    val ipSourceHitDeviceIdDayQuery = ipSourceHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("IPSourceHitDeviceIdPerDay")
      .foreach(writerIPSourceHitDeviceIdDay)
      .start()

    val ipDestHitCompanySecQuery = ipDestHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitCompanyPerSec")
      .foreach(writerIPDestHitCompanySec)
      .start()

    val ipDestHitCompanyMinQuery = ipDestHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitCompanyPerMin")
      .foreach(writerIPDestHitCompanyMin)
      .start()

    val ipDestHitCompanyHourQuery = ipDestHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitCompanyPerHour")
      .foreach(writerIPDestHitCompanyHour)
      .start()

    val ipDestHitCompanyDayQuery = ipDestHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitCompanyPerDay")
      .foreach(writerIPDestHitCompanyDay)
      .start()

    val ipDestHitDeviceIdSecQuery = ipDestHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitDeviceIdPerSec")
      .foreach(writerIPDestHitDeviceIdSec)
      .start()

    val ipDestHitDeviceIdMinQuery = ipDestHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitDeviceIdPerMin")
      .foreach(writerIPDestHitDeviceIdMin)
      .start()

    val ipDestHitDeviceIdHourQuery = ipDestHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitDeviceIdPerHour")
      .foreach(writerIPDestHitDeviceIdHour)
      .start()

    val ipDestHitDeviceIdDayQuery = ipDestHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("IPDestHitDeviceIdPerDay")
      .foreach(writerIPDestHitDeviceIdDay)
      .start()

    val countrySourceHitCompanySecQuery = countrySourceHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitCompanyPerSec")
      .foreach(writerCountrySrcHitCompanySec)
      .start()

    val countrySourceHitCompanyMinQuery = countrySourceHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitCompanyPerMin")
      .foreach(writerCountrySrcHitCompanyMin)
      .start()

    val countrySourceHitCompanyHourQuery = countrySourceHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitCompanyPerHour")
      .foreach(writerCountrySrcHitCompanyHour)
      .start()

    val countrySourceHitCompanyDayQuery = countrySourceHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitCompanyPerDay")
      .foreach(writerCountrySrcHitCompanyDay)
      .start()

    val countrySourceHitDeviceIdSecQuery = countrySourceHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitDeviceIdPerSec")
      .foreach(writerCountrySrcHitDeviceIdSec)
      .start()

    val countrySourceHitDeviceIdMinQuery = countrySourceHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitDeviceIdPerMin")
      .foreach(writerCountrySrcHitDeviceIdMin)
      .start()

    val countrySourceHitDeviceIdHourQuery = countrySourceHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitDeviceIdPerHour")
      .foreach(writerCountrySrcHitDeviceIdHour)
      .start()

    val countrySourceHitDeviceIdDayQuery = countrySourceHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("CountrySourceHitDeviceIdPerDay")
      .foreach(writerCountrySrcHitDeviceIdDay)
      .start()

    val countryDestHitCompanySecQuery = countryDestHitCompanySecDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitCompanyPerSec")
      .foreach(writerCountryDestHitCompanySec)
      .start()

    val countryDestHitCompanyMinQuery = countryDestHitCompanyMinDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitCompanyPerMin")
      .foreach(writerCountryDestHitCompanyMin)
      .start()

    val countryDestHitCompanyHourQuery = countryDestHitCompanyHourDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitCompanyPerHour")
      .foreach(writerCountryDestHitCompanyHour)
      .start()

    val countryDestHitCompanyDayQuery = countryDestHitCompanyDayDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitCompanyPerDay")
      .foreach(writerCountryDestHitCompanyDay)
      .start()

    val countryDestHitDeviceIdSecQuery = countryDestHitDeviceIdSecDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitDeviceIdPerSec")
      .foreach(writerCountryDestHitDeviceIdSec)
      .start()

    val countryDestHitDeviceIdMinQuery = countryDestHitDeviceIdMinDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitDeviceIdPerMin")
      .foreach(writerCountryDestHitDeviceIdMin)
      .start()

    val countryDestHitDeviceIdHourQuery = countryDestHitDeviceIdHourDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitDeviceIdPerHour")
      .foreach(writerCountryDestHitDeviceIdHour)
      .start()

    val countryDestHitDeviceIdDayQuery = countryDestHitDeviceIdDayDs
      .writeStream
      .outputMode("update")
      .queryName("CountryDestHitDeviceIdPerDay")
      .foreach(writerCountryDestHitDeviceIdDay)
      .start()

    ipSourceHitCompanySecQuery.awaitTermination()
    ipSourceHitCompanyMinQuery.awaitTermination()
    ipSourceHitCompanyHourQuery.awaitTermination()
    ipSourceHitCompanyDayQuery.awaitTermination()
    ipSourceHitDeviceIdSecQuery.awaitTermination()
    ipSourceHitDeviceIdMinQuery.awaitTermination()
    ipSourceHitDeviceIdHourQuery.awaitTermination()
    ipSourceHitDeviceIdDayQuery.awaitTermination()
    ipDestHitCompanySecQuery.awaitTermination()
    ipDestHitCompanyMinQuery.awaitTermination()
    ipDestHitCompanyHourQuery.awaitTermination()
    ipDestHitCompanyDayQuery.awaitTermination()
    ipDestHitDeviceIdSecQuery.awaitTermination()
    ipDestHitDeviceIdMinQuery.awaitTermination()
    ipDestHitDeviceIdHourQuery.awaitTermination()
    ipDestHitDeviceIdDayQuery.awaitTermination()
    countryDestHitCompanySecQuery.awaitTermination()
    countryDestHitCompanyMinQuery.awaitTermination()
    countryDestHitCompanyHourQuery.awaitTermination()
    countryDestHitCompanyDayQuery.awaitTermination()
    countryDestHitDeviceIdSecQuery.awaitTermination()
    countryDestHitDeviceIdMinQuery.awaitTermination()
    countryDestHitDeviceIdHourQuery.awaitTermination()
    countryDestHitDeviceIdDayQuery.awaitTermination()
    countrySourceHitCompanySecQuery.awaitTermination()
    countrySourceHitCompanyMinQuery.awaitTermination()
    countrySourceHitCompanyHourQuery.awaitTermination()
    countrySourceHitCompanyDayQuery.awaitTermination()
    countrySourceHitDeviceIdSecQuery.awaitTermination()
    countrySourceHitDeviceIdMinQuery.awaitTermination()
    countrySourceHitDeviceIdHourQuery.awaitTermination()
    countrySourceHitDeviceIdDayQuery.awaitTermination()
  }
}
