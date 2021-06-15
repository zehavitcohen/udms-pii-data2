package com.undertone.udms.logsagg.utils

import java.net.{URI, URLDecoder, URLEncoder}

import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.{NameValuePair => ApacheNameValuePair}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

object HandleQueryString {
  def main(args: Array[String]): Unit = {
    val result = URLEncoder.encode("/l?bannerid=1438756&campaignid=356635&zoneid=190143&ptm=129143|37474|37475|70488|70487|129139|2231|126013|126021&cb=1b789cb402f14af09316130e3f2a1d71&bk=q2lbm2&id=ca62vefmfcteviziiprc88yka&stid=231&uasv=v22&st=https%3A%2F%2Fwww.slader.com%2Ftextbook%2F9780073398235-mechanics-of-materials-7th-edition%2F178%2Fproblems%2F38%2F&slice=eJyrViooMVCyqlZKTCnNyyyJz0xRslKyMFHSUUopiU9MKSjKzC/KLKkECqLywfIp+bmJmXlAueKcxJTUIr3k/FyIRAHYGGMLAzMIvyQxHSxiAOQW52Qmp3pCeLW1ALtNKT0=&pid=3806&bidid=ca62vefmfcteviziiprc88yka&dtver=2&deimp=1&ut_pii_allowed=1&has_capping=0")
    println(result)
    println(URLDecoder.decode(result))
    println(getMapFromURI(URLDecoder.decode(result)))
  }

  def getMapFromURI(url: String): Map[String,String] = {
    val params = URLEncodedUtils.parse(new URI(url), "UTF8")

    val convertedParams: Seq[ApacheNameValuePair] = collection.immutable.Seq(params.asScala: _*)
    val scalaParams: Seq[(String, String)] = convertedParams.map(pair => pair.getName -> pair.getValue)
    scalaParams.toMap
  }
}
