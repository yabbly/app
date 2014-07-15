package com.studio6.app.common

import org.apache.commons.codec.binary.{Base64 => B64}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.{RandomStringUtils => RSU}

import java.util.Random

object SecurityUtils {
  private def random = new Random

  /**
   * @return (salt, encryptedPassword)
   */
  def encryptPassword(clear: String): (String, String) = {
    val salt = SecurityUtils.randomAlphanumeric(16)
    (salt, encryptPassword(clear, salt))
  }

  def encryptPassword(clear: String, salt: String): String = {
    var encPassword = "[%s]-[%s]".format(clear, salt)
    1.to(8).foreach(i => {
      encPassword = sha512Hex(encPassword)
    })
    encPassword
  }

  def base64Encode(bs: Array[Byte]): String = B64.encodeBase64String(bs)

  def base64Encode(s: String): Array[Byte] = B64.decodeBase64(s)

  def base64EncodeUrlSafe(bs: Array[Byte]): String = B64.encodeBase64URLSafeString(bs)

  def base64EncodeUrlSafe(s: String): Array[Byte] = B64.decodeBase64(s)

  def utf8Encode(bs: Array[Byte]): String = new String(bs, "utf-8")

  def utf8Encode(s: String): Array[Byte] = s.getBytes("utf-8")

  def iso88591Encode(bs: Array[Byte]): String = new String(bs, "iso-8859-1")

  def iso88591Encode(s: String): Array[Byte] = s.getBytes("iso-8859-1")

  def md5Hex(s: String): String = DigestUtils.md5Hex(s)

  def md5Hex(bs: Array[Byte]): String = DigestUtils.md5Hex(bs)

  def sha256Hex(s: String): String = DigestUtils.sha256Hex(s)

  def sha256Hex(bs: Array[Byte]): String = DigestUtils.sha256Hex(bs)

  def sha512Hex(s: String): String = DigestUtils.sha512Hex(s)

  def sha512Hex(bs: Array[Byte]): String = DigestUtils.sha512Hex(bs)

  def randomAlphanumeric(length: Int): String = RSU.randomAlphanumeric(length)

  def randomPositiveInt(): Int = Math.abs(random.nextInt());
}
