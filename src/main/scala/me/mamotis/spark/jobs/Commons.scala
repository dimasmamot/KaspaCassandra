package me.mamotis.spark.jobs

object Commons {
  case class EventObj(device_id: String, year: Integer, month: Integer,
                      day: Integer, hour: Integer, minute: Integer,
                      second: Integer, protocol: String, ip_type: String,
                      src_mac: String, dest_mac: String, src_ip: String,
                      dest_ip: String, src_port: Long, dst_port: Long,
                      alert_msg: String, classification: Long ,
                      priority: Long, sig_id: Long, sig_gen: Long,
                      sig_rev: Long, src_country: String) extends Serializable
}