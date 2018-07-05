package me.mamotis.spark.jobs

object Commons {

//  Event Related Object
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

//  Signature Related Obj

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

//  Protocol Related Obj

  case class ProtocolHitCompanyObjSec(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolHitCompanyObjMin(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                      minute: Integer, value: Long) extends Serializable

  case class ProtocolHitCompanyObjHour(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       value: Long) extends Serializable

  case class ProtocolHitCompanyObjDay(company: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjSec(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjMin(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                       minute: Integer, value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjHour(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer,
                                        value: Long) extends Serializable

  case class ProtocolHitDeviceIdObjDay(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

//  Protocol + Port Related Obj

  case class ProtocolBySPortHitCompanyObjSec(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjMin(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjHour(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              value: Long) extends Serializable

  case class ProtocolBySPortHitCompanyObjDay(company: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjSec(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjMin(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjHour(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                               value: Long) extends Serializable

  case class ProtocolBySPortHitDeviceIdObjDay(device_id: String, protocol: String, src_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjSec(company: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjMin(company: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                             minute: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjHour(company: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              value: Long) extends Serializable

  case class ProtocolByDPortHitCompanyObjDay(company: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjSec(device_id: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, second: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjMin(device_id: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                              minute: Integer, value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjHour(device_id: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, hour: Integer,
                                               value: Long) extends Serializable

  case class ProtocolByDPortHitDeviceIdObjDay(device_id: String, protocol: String, dst_port: Integer, year: Integer, month: Integer, day: Integer, value: Long) extends Serializable

}
