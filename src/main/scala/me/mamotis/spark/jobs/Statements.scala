package me.mamotis.spark.jobs

import java.util.UUID

object Statements extends Serializable {

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

  def push_event_hit_company_month(company: String, year: Integer, month: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_sec ("company", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', $year, $month, $value)
                                       """.stripMargin

  def push_event_hit_company_year(company: String, year: Integer, value: Long): String =
                                      s"""
                                         |INSERT INTO kaspa.event_hit_on_company_sec ("company", "year", "month", "day",
                                         |"hour", "minute", "second", "value") values ('$company', $year, $value)
                                       """.stripMargin
}
