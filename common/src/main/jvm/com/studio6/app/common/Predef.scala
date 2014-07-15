package com.studio6.app.common

object Predef {

  implicit class RichString(s: String) {
    def toUtf8Bytes: Array[Byte] = SecurityUtils.utf8Encode(s)
  }

  implicit class RichLong(l: Long) {
    def toOption: Option[Long] = Some(l)
    def toUtf8Bytes: Array[Byte] = SecurityUtils.utf8Encode(l.toString)
  }

  implicit class RichInt(i: Int) {
    def toOption: Option[Int] = Some(i)
    def toUtf8Bytes: Array[Byte] = SecurityUtils.utf8Encode(i.toString)
  }
}
