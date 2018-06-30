package me.mamotis.spark.jobs

import java.util.UUID

object Statements extends Serializable {

  def cql(id: UUID, device_id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
          minute: Integer, second: Integer, protocol: String, ip_type: String, src_mac: String,
          dest_mac: String, src_ip: String, dest_ip: String, src_port: Long, dst_port: Long,
          alert_msg: String, classification: Long, priority: Long, sig_id: Long,
          sig_gen: Long, sig_rev: Long, src_country: String): String =
            s"""
               |INSERT INTO kaspa.raw_data ("id", "device_id", "year", "month", "day", "hour", "minute", "second",
               |"protocol", "ip_type", "src_mac", "dest_mac", "src_ip", "dest_ip", "src_port", "dst_port",
               |"alert_msg", "classification", "priority", "sig_id", "sig_gen", "sig_rev", "src_country")
               |values($id, '$device_id', $year, $month, $day, $hour, $minute, $second, '$protocol',
               |'$ip_type', '$src_mac', '$dest_mac', '$src_ip', '$dest_ip',$src_port, $dst_port, '$alert_msg',
               |$classification, $priority, $sig_id, $sig_gen, $sig_rev, '$src_country')
             """.stripMargin

}
