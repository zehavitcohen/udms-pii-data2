package com.undertone.udms.logsagg.dto

case class LogEventTypeSchema(intervalStart: Long, zoneId: Int, bannerId: Int, eventType: String, count: Long)
