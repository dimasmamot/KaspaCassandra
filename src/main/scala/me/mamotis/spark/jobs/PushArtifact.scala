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

  def pushEventHitCompanySec(value: Commons.EventHitCompanyObjSec, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_second(value.company, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.value))
    }
  }

  def pushEventHitCompanyMin(value: Commons.EventHitCompanyObjMin, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_minute(value.company, value.year, value.month, value.day, value.hour,
          value.minute, value.value))
    }
  }

  def pushEventHitCompanyHour(value: Commons.EventHitCompanyObjHour, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_hour(value.company, value.year, value.month, value.day, value.hour, value.value))
    }
  }

  def pushEventHitCompanyDay(value: Commons.EventHitCompanyObjDay, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_company_day(value.company, value.year, value.month, value.day, value.value))
    }
  }

  def pushEventHitDeviceIdSec(value: Commons.EventHitDeviceIdObjSec, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_device_id_second(value.device_id, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.value))
    }
  }

  def pushEventHitDeviceIdMin(value: Commons.EventHitDeviceIdObjMin, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_device_id_minute(value.device_id, value.year, value.month, value.day, value.hour,
          value.minute, value.value))
    }
  }

  def pushEventHitDeviceIdHour(value: Commons.EventHitDeviceIdObjHour, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_device_id_hour(value.device_id, value.year, value.month, value.day, value.hour, value.value))
    }
  }

  def pushEventHitDeviceIdDay(value: Commons.EventHitDeviceIdObjDay, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_event_hit_device_id_day(value.device_id, value.year, value.month, value.day, value.value))
    }
  }

  def pushSignatureHitCompanySec(value: Commons.SignatureHitCompanyObjSec, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_company_second(value.company, value.alert_msg, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.value))
    }
  }

  def pushSignatureHitCompanyMin(value: Commons.SignatureHitCompanyObjMin, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_company_minute(value.company, value.alert_msg, value.year, value.month, value.day, value.hour,
          value.minute, value.value))
    }
  }

  def pushSignatureHitCompanyHour(value: Commons.SignatureHitCompanyObjHour, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_company_hour(value.company, value.alert_msg, value.year, value.month, value.day, value.hour, value.value))
    }
  }

  def pushSignatureHitCompanyDay(value: Commons.SignatureHitCompanyObjDay, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_company_day(value.company, value.alert_msg, value.year, value.month, value.day, value.value))
    }
  }

  def pushSignatureHitDeviceIdSec(value: Commons.SignatureHitDeviceIdObjSec, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_device_id_second(value.device_id, value.alert_msg, value.year, value.month, value.day, value.hour,
          value.minute, value.second, value.value))
    }
  }

  def pushSignatureHitDeviceIdMin(value: Commons.SignatureHitDeviceIdObjMin, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_device_id_minute(value.device_id, value.alert_msg, value.year, value.month, value.day, value.hour,
          value.minute, value.value))
    }
  }

  def pushSignatureHitDeviceIdHour(value: Commons.SignatureHitDeviceIdObjHour, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_device_id_hour(value.device_id, value.alert_msg, value.year, value.month, value.day, value.hour, value.value))
    }
  }

  def pushSignatureHitDeviceIdDay(value: Commons.SignatureHitDeviceIdObjDay, connector: CassandraConnector) = {
    connector.withSessionDo{
      session =>
        session.execute(Statements.push_signature_hit_device_id_day(value.device_id, value.alert_msg, value.year, value.month, value.day, value.value))
    }
  }
}
