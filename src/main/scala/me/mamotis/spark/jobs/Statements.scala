package me.mamotis.spark.jobs

import java.util.UUID

object Statements extends Serializable {

//  Event hit query related

  def push_raw_data_by_company(id: UUID, company:String, device_id: String, year: Integer, month: Integer, day: Integer,
                     hour: Integer, minute: Integer, second: Integer, protocol: String, ip_type: String, src_mac: String,
                     dest_mac: String, src_ip: String, dest_ip: String, src_port: Integer, dst_port: Integer,
                     alert_msg: String, classification: Integer, priority: Integer, sig_id: Integer,
                     sig_gen: Integer, sig_rev: Integer, src_country: String): String =
                        s"""
                           |INSERT INTO kaspa.raw_data_by_company ("id", "company", "device_id", "year", "month", "day", "hour", "minute", "second",
                           |"protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip", "src_port", "dst_port",
                           |"alert_msg", "classification", "priority", "sig_id", "sig_gen", "sig_rev", "src_country")
                           |values($id, '$company', '$device_id', $year, $month, $day, $hour, $minute, $second, '$protocol',
                           |'$ip_type', '$src_mac', '$dest_mac', '$src_ip', '$dest_ip',$src_port, $dst_port, '$alert_msg',
                           |$classification, $priority, $sig_id, $sig_gen, $sig_rev, '$src_country')
                         """.stripMargin

  def push_raw_data_by_device_id(id: UUID, company:String, device_id: String, year: Integer, month: Integer, day: Integer,
                               hour: Integer, minute: Integer, second: Integer, protocol: String, ip_type: String, src_mac: String,
                               dest_mac: String, src_ip: String, dest_ip: String, src_port: Integer, dst_port: Integer,
                               alert_msg: String, classification: Integer, priority: Integer, sig_id: Integer,
                               sig_gen: Integer, sig_rev: Integer, src_country: String): String =
                                  s"""
                                     |INSERT INTO kaspa.raw_data_by_device_id ("id", "company", "device_id", "year", "month", "day", "hour", "minute", "second",
                                     |"protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip", "src_port", "dst_port",
                                     |"alert_msg", "classification", "priority", "sig_id", "sig_gen", "sig_rev", "src_country")
                                     |values($id, '$company', '$device_id', $year, $month, $day, $hour, $minute, $second, '$protocol',
                                     |'$ip_type', '$src_mac', '$dest_mac', '$src_ip', '$dest_ip',$src_port, $dst_port, '$alert_msg',
                                     |$classification, $priority, $sig_id, $sig_gen, $sig_rev, '$src_country')
                                  """.stripMargin

  def push_event_hit_company_second(company: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                    second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_sec ("company", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_event_hit_company_minute(company: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                    value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_minute ("company", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$company', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_event_hit_company_hour(company: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_hour ("company", "year", "month", "day",
                                         |"hour", "value") values ('$company', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_event_hit_company_day(company: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_day ("company", "year", "month", "day",
                                         |"value") values ('$company', $year, $month, $day, $value)
                                       """.stripMargin

  def push_event_hit_device_id_second(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                      second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_device_id_sec ("device_id", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$device_id', $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_event_hit_device_id_minute(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                      value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_device_id_minute ("device_id", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$device_id', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_event_hit_device_id_hour(device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_device_id_hour ("device_id", "year", "month", "day",
                                         |"hour", "value") values ('$device_id', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_event_hit_device_id_day(device_id: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_device_id_day ("device_id", "year", "month", "day",
                                         |"value") values ('$device_id', $year, $month, $day, $value)
                                       """.stripMargin

//  Signature hit query related

  def push_signature_hit_company_second(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                    second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_company_sec ("company", "alert_msg", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', '$alert_msg', $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_signature_hit_company_minute(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                        value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_company_minute ("company", "alert_msg", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$company', '$alert_msg', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_signature_hit_company_hour(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_company_hour ("company", "alert_msg", "year", "month", "day",
                                         |"hour", "value") values ('$company', '$alert_msg', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_signature_hit_company_day(company: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_company_day ("company", "alert_msg", "year", "month", "day",
                                         |"value") values ('$company', '$alert_msg', $year, $month, $day, $value)
                                       """.stripMargin

  def push_signature_hit_device_id_second(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                          second: Integer, value: Long): String =
                                        s"""
                                           |INSERT INTO kaspa.signature_hit_on_device_id_sec ("device_id", "alert_msg", "year", "month", "day",
                                           |"hour", "minute", "second", "value") values ('$device_id', '$alert_msg', $year, $month,
                                           |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_signature_hit_device_id_minute(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                          value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_device_id_minute ("device_id", "alert_msg", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$device_id', '$alert_msg', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_signature_hit_device_id_hour(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_device_id_hour ("device_id", "alert_msg", "year", "month", "day",
                                         |"hour", "value") values ('$device_id', '$alert_msg', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_signature_hit_device_id_day(device_id: String, alert_msg: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.signature_hit_on_device_id_day ("device_id", "alert_msg", "year", "month", "day",
                                         |"value") values ('$device_id', '$alert_msg', $year, $month, $day, $value)
                                       """.stripMargin

//  Protocol hit query related

  def push_protocol_hit_company_second(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                       second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_company_sec ("company", "protocol", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', '$protocol', $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_hit_company_minute(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                       value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_company_minute ("company", "protocol", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$company', '$protocol', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_hit_company_hour(company: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_company_hour ("company", "protocol", "year", "month", "day",
                                         |"hour", "value") values ('$company', '$protocol', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_hit_company_day(company: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_company_day ("company", "protocol", "year", "month", "day",
                                         |"value") values ('$company', '$protocol', $year, $month, $day, $value)
                                       """.stripMargin

  def push_protocol_hit_device_id_second(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                         second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_device_id_sec ("device_id", "protocol", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$device_id', '$protocol', $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_hit_device_id_minute(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                         value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_device_id_minute ("device_id", "protocol", "year", "month", "day",
                                         |"hour", "minute", "value") values ('$device_id', '$protocol', $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_hit_device_id_hour(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_device_id_hour ("device_id", "protocol", "year", "month", "day",
                                         |"hour", "value") values ('$device_id', '$protocol', $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_hit_device_id_day(device_id: String, protocol: String, year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_hit_on_device_id_day ("device_id", "protocol", "year", "month", "day",
                                         |"value") values ('$device_id', '$protocol', $year, $month, $day, $value)
                                       """.stripMargin

//  Protocol + Port Query Related

  def push_protocol_by_sport_hit_company_second(company: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_company_sec ("company", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', '$protocol', $src_port, $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_company_minute(company: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_company_minute ("company", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "minute", "value") values ('$company', '$protocol', $src_port, $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_company_hour(company: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_company_hour ("company", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "value") values ('$company', '$protocol', $src_port, $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_company_day(company: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_company_day ("company", "protocol", "src_port,  "year", "month", "day",
                                         |"value") values ('$company', '$protocol', $src_port, $year, $month, $day, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_device_id_second(device_id: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                  second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_device_id_sec ("device_id", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$device_id', '$protocol', $src_port, $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_device_id_minute(device_id: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                  value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_device_id_minute ("device_id", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "minute", "value") values ('$device_id', '$protocol', $src_port, $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_device_id_hour(device_id: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_device_id_hour ("device_id", "protocol", "src_port,  "year", "month", "day",
                                         |"hour", "value") values ('$device_id', '$protocol', $src_port, $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_by_sport_hit_device_id_day(device_id: String, protocol: String, src_port: Integer,  year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_sport_hit_on_device_id_day ("device_id", "protocol", "src_port,  "year", "month", "day",
                                         |"value") values ('$device_id', '$protocol', $src_port, $year, $month, $day, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_company_second(company: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_company_sec ("company", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', '$protocol', $dst_port, $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_company_minute(company: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_company_minute ("company", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "minute", "value") values ('$company', '$protocol', $dst_port, $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_company_hour(company: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_company_hour ("company", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "value") values ('$company', '$protocol', $dst_port, $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_company_day(company: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_company_day ("company", "protocol", "dst_port,  "year", "month", "day",
                                         |"value") values ('$company', '$protocol', $dst_port, $year, $month, $day, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_device_id_second(device_id: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                  second: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_device_id_sec ("device_id", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$device_id', '$protocol', $dst_port, $year, $month,
                                         |$day, $hour, $minute, $second, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_device_id_minute(device_id: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, minute: Integer,
                                                  value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_device_id_minute ("device_id", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "minute", "value") values ('$device_id', '$protocol', $dst_port, $year, $month,
                                         |$day, $hour, $minute, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_device_id_hour(device_id: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, hour: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_device_id_hour ("device_id", "protocol", "dst_port,  "year", "month", "day",
                                         |"hour", "value") values ('$device_id', '$protocol', $dst_port, $year, $month, $day, $hour, $value)
                                       """.stripMargin

  def push_protocol_by_dport_hit_device_id_day(device_id: String, protocol: String, dst_port: Integer,  year: Integer, month: Integer, day: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.protocol_by_dport_hit_on_device_id_day ("device_id", "protocol", "dst_port,  "year", "month", "day",
                                         |"value") values ('$device_id', '$protocol', $dst_port, $year, $month, $day, $value)
                                       """.stripMargin
}
