package me.mamotis.spark.jobs

object ColsArtifact {
  val colsEventObj = List("company", "device_id", "year", "month", "day", "hour", "minute", "second",
    "protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip",
    "src_port", "dst_port", "alert_msg", "classification", "priority",
    "sig_id", "sig_gen", "sig_rev", "src_country")

  val colsEventHitObjSec = List("company", "year", "month", "day", "hour", "minute", "second", "value")

  val colsEventHitObjMin = List("company", "year", "month", "day", "hour", "minute", "value")

  val colsEventHitObjHour = List("company", "year", "month", "day", "hour", "value")

  val colsEventHitObjDay = List("company", "year", "month", "day", "value")

  val colsEventHitObjMonth = List("company", "year", "month", "value")

  val colsEventHitObjYear = List("company", "year", "value")
}
