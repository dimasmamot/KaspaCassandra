package me.mamotis.spark.jobs

object ColsArtifact {
  val colsEventObj = List("company", "device_id", "year", "month", "day", "hour", "minute", "second",
    "protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip",
    "src_port", "dst_port", "alert_msg", "classification", "priority",
    "sig_id", "sig_gen", "sig_rev", "src_country")

  val colsEventHitCompanyObjSec = List("company", "year", "month", "day", "hour", "minute", "second", "value")

  val colsEventHitCompanyObjMin = List("company", "year", "month", "day", "hour", "minute", "value")

  val colsEventHitCompanyObjHour = List("company", "year", "month", "day", "hour", "value")

  val colsEventHitCompanyObjDay = List("company", "year", "month", "day", "value")

  val colsEventHitDeviceIdObjSec = List("device_id", "year", "month", "day", "hour", "minute", "second", "value")

  val colsEventHitDeviceIdObjMin = List("device_id", "year", "month", "day", "hour", "minute", "value")

  val colsEventHitDeviceIdObjHour = List("device_id", "year", "month", "day", "hour", "value")

  val colsEventHitDeviceIdObjDay = List("device_id", "year", "month", "day", "value")

  val colsSignatureHitCompanyObjSec = List("company", "alert_msg", "year", "month", "day", "hour", "minute", "second", "value")

  val colsSignatureHitCompanyObjMin = List("company", "alert_msg", "year", "month", "day", "hour", "minute", "value")

  val colsSignatureHitCompanyObjHour = List("company", "alert_msg", "year", "month", "day", "hour", "value")

  val colsSignatureHitCompanyObjDay = List("company", "alert_msg", "year", "month", "day", "value")

  val colsSignatureHitDeviceIdObjSec = List("device_id", "alert_msg", "year", "month", "day", "hour", "minute", "second", "value")

  val colsSignatureHitDeviceIdObjMin = List("device_id", "alert_msg", "year", "month", "day", "hour", "minute", "value")

  val colsSignatureHitDeviceIdObjHour = List("device_id", "alert_msg", "year", "month", "day", "hour", "value")

  val colsSignatureHitDeviceIdObjDay = List("device_id", "alert_msg", "year", "month", "day", "value")
}
