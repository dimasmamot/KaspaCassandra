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

  case class EventHitCompanyObjSec(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                            minute: Integer, second: Integer, value: Long) extends Serializable

  case class EventHitCompanyObjMin(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                            minute: Integer, value: Long) extends Serializable

  case class EventHitCompanyObjHour(company: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                             value: Long) extends Serializable

  case class EventHitCompanyObjDay(company: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjSec(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, second: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjMin(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                    minute: Integer, value: Long) extends Serializable

  case class EventHitDeviceIdObjHour(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                     value: Long) extends Serializable

  case class EventHitDeviceIdObjDay(device_id: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class SignatureHitCompanyObjSec(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, second: Integer, value: Long) extends Serializable

  case class SignatureHitCompanyObjMin(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, value: Long) extends Serializable

  case class SignatureHitCompanyObjHour(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        value: Long) extends Serializable

  case class SignatureHitCompanyObjDay(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjSec(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, second: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjMin(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        minute: Integer, value: Long) extends Serializable

  case class SignatureHitDeviceIdObjHour(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                         value: Long) extends Serializable

  case class SignatureHitDeviceIdObjDay(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable
}
