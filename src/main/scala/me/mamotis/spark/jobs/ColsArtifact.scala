package me.mamotis.spark.jobs

object ColsArtifact {

//  Event Hit
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

//  Signature Hit

  val colsSignatureHitCompanyObjSec = List("company", "alert_msg", "year", "month", "day", "hour", "minute", "second", "value")

  val colsSignatureHitCompanyObjMin = List("company", "alert_msg", "year", "month", "day", "hour", "minute", "value")

  val colsSignatureHitCompanyObjHour = List("company", "alert_msg", "year", "month", "day", "hour", "value")

  val colsSignatureHitCompanyObjDay = List("company", "alert_msg", "year", "month", "day", "value")

  val colsSignatureHitDeviceIdObjSec = List("device_id", "alert_msg", "year", "month", "day", "hour", "minute", "second", "value")

  val colsSignatureHitDeviceIdObjMin = List("device_id", "alert_msg", "year", "month", "day", "hour", "minute", "value")

  val colsSignatureHitDeviceIdObjHour = List("device_id", "alert_msg", "year", "month", "day", "hour", "value")

  val colsSignatureHitDeviceIdObjDay = List("device_id", "alert_msg", "year", "month", "day", "value")

//  Protocol Hit

  val colsProtocolHitCompanyObjSec = List("company", "protocol", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolHitCompanyObjMin = List("company", "protocol", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolHitCompanyObjHour = List("company", "protocol", "year", "month", "day", "hour", "value")

  val colsProtocolHitCompanyObjDay = List("company", "protocol", "year", "month", "day", "value")

  val colsProtocolHitDeviceIdObjSec = List("device_id", "protocol", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolHitDeviceIdObjMin = List("device_id", "protocol", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolHitDeviceIdObjHour = List("device_id", "protocol", "year", "month", "day", "hour", "value")

  val colsProtocolHitDeviceIdObjDay = List("device_id", "protocol", "year", "month", "day", "value")

//  Protocol + Port Hit

  val colsProtocolBySPortHitCompanyObjSec = List("company", "protocol", "src_port", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolBySPortHitCompanyObjMin = List("company", "protocol", "src_port", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolBySPortHitCompanyObjHour = List("company", "protocol", "src_port", "year", "month", "day", "hour", "value")

  val colsProtocolBySPortHitCompanyObjDay = List("company", "protocol", "src_port", "year", "month", "day", "value")

  val colsProtocolBySPortHitDeviceIdObjSec = List("device_id", "protocol", "src_port", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolBySPortHitDeviceIdObjMin = List("device_id", "protocol", "src_port", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolBySPortHitDeviceIdObjHour = List("device_id", "protocol", "src_port", "year", "month", "day", "hour", "value")

  val colsProtocolBySPortHitDeviceIdObjDay = List("device_id", "protocol", "src_port", "year", "month", "day", "value")

  val colsProtocolByDPortHitCompanyObjSec = List("company", "protocol", "dst_port", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolByDPortHitCompanyObjMin = List("company", "protocol", "dst_port", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolByDPortHitCompanyObjHour = List("company", "protocol", "dst_port", "year", "month", "day", "hour", "value")

  val colsProtocolByDPortHitCompanyObjDay = List("company", "protocol", "dst_port", "year", "month", "day", "value")

  val colsProtocolByDPortHitDeviceIdObjSec = List("device_id", "protocol", "dst_port", "year", "month", "day", "hour", "minute", "second", "value")

  val colsProtocolByDPortHitDeviceIdObjMin = List("device_id", "protocol", "dst_port", "year", "month", "day", "hour", "minute", "value")

  val colsProtocolByDPortHitDeviceIdObjHour = List("device_id", "protocol", "dst_port", "year", "month", "day", "hour", "value")

  val colsProtocolByDPortHitDeviceIdObjDay = List("device_id", "protocol", "dst_port",  "year", "month", "day", "value")

}
