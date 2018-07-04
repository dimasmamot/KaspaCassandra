package me.mamotis.spark.jobs

object Commons {
  case class EventObj(company: String, device_id: String, year: Integer, month: Integer,
                      day: Integer, hour: Integer, minute: Integer,
                      second: Integer, protocol: String, ip_type: String,
                      src_mac: String, dest_mac: String, src_ip: String,
                      dest_ip: String, src_port: Integer, dst_port: Integer,
                      alert_msg: String, classification: Integer,
                      priority: Integer, sig_id: Integer, sig_gen: Integer,
                      sig_rev: Integer, src_country: String) extends Serializable

  case class EventHitObjSec(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                            minute: Integer, second: Integer, value: Long) extends Serializable

  case class EventHitObjMin(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                            minute: Integer, value: Long) extends Serializable

  case class EventHitObjHour(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                             value: Long) extends Serializable

  case class EventHitObjDay(company: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class EventHitObjMonth(company: String, year: Integer, month: Integer, value: Long) extends Serializable

  case class EventHitObjYear(company: String, year: Integer, value: Long) extends Serializable
}
