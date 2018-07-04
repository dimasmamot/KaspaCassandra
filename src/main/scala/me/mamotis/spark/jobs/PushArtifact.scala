package me.mamotis.spark.jobs

import java.util.UUID.randomUUID

import com.datastax.spark.connector.cql.CassandraConnector

object PushArtifact {
  def pushRawData(value: Commons.EventObj, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_raw_data_by_company(randomUUID(), value.company, value.device_id, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.protocol, value.ip_type, value.src_mac, value.dest_mac,
          value.src_ip, value.dest_ip, value.src_port, value.dst_port, value.alert_msg, value.classification,
          value.priority, value.sig_id, value.sig_gen, value.sig_rev, value.src_country))

        session.execute(Statements.push_raw_data_by_device_id(randomUUID(), value.company, value.device_id, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.protocol, value.ip_type, value.src_mac, value.dest_mac,
          value.src_ip, value.dest_ip, value.src_port, value.dst_port, value.alert_msg, value.classification,
          value.priority, value.sig_id, value.sig_gen, value.sig_rev, value.src_country))
    }
  }

  def pushEventHitCompanySec(value: Commons.EventHitObjSec, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_second(value.company, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.value))
    }
  }

  def pushEventHitCompanyMin(value: Commons.EventHitObjMin, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_minute(value.company, value.year, value.month, value.day, value.hour,
          value.minute, value.value))
    }
  }

  def pushEventHitCompanyHour(value: Commons.EventHitObjHour, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_hour(value.company, value.year, value.month, value.day, value.hour, value.value))
    }
  }

  def pushEventHitCompanyDay(value: Commons.EventHitObjDay, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_day(value.company, value.year, value.month, value.day, value.value))
    }
  }

  def pushEventHitCompanyMonth(value: Commons.EventHitObjMonth, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_month(value.company, value.year, value.month, value.value))
    }
  }

  def pushEventHitCompanyYear(value: Commons.EventHitObjYear, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_year(value.company, value.year, value.value))
    }
  }
}
