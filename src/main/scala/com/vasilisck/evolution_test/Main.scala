package com.vasilisck.evolution_test

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object Main extends App {

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("Evolution Gaming Test")
    .set("spark.local.dir", "/tmp/spark-temp")

  val spark = SparkSession.builder.config(sparkConf).getOrCreate()


  /** Specification of nginx logs:
    * Data written in specific order. If data is not present, then instead of it writen "-"
    * 1)Ip address of the web client.
    * 2)Name of remote log. Probably not so necessary.
    * 3)Username the web client was authorized under. Necessity depends on case.
    * 4)Date and time of access.
    * 5)First line of the request the web client sent to the server.
    * 6)Status code of response.
    * 7)Size of response. Necessity depends on case.
    * 8)Referrer Url. Necessity depends on case.
    * 9)User agent.
    *
    * This was used for proper creation of case class of record.
    */

  /** For user agent i decide to not trying to parse it because there is no specification for it and it will probably
    * take a lot of time to do it. Like requests can be without browser name at first place and i can't imagine way
    * to identify is record is browser data or something else without full .
    */

  import spark.implicits._
  import org.apache.spark.sql.functions._

  //first implementation. We can you both repartition and  coalesce for it. Mapping happen on slaves nodes and after it
  //all the data travel to master node.
  val basicData = spark.read.textFile("nginx_logs")
    .map(parseNginxRecord)

  basicData
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .format("json")
    .save("./out/result_basic")
  //result size is 12.09 MB which is bigger, that raw data. But we got a nice Json

  //second version. Let's try only useful data (by my opinion) and save it with repartition
  val liteData = basicData.map(nginxRecord =>
    NginxRecordLite(nginxRecord.ipAddress, nginxRecord.date, nginxRecord.requestData, nginxRecord.responseCode, nginxRecord.userAgent))

  liteData
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .format("json")
    .save("./out/result_lite")

  //now let's write result data where partision will be done by IP address
  val res = liteData.groupByKey(_.ipAddress)
    .mapGroups { case (key, vs) =>
        key -> vs.toList
    }
    .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
    .format("json")
    .save("./out/result_partisioned")

  println(res)

  val q = 0
  q



  spark.close()

  def parseRequestData(requestData: String): RequestData = {
    val attrs = requestData.split(" ")
    RequestData(attrs(0), attrs(1), attrs(2))
  }

  def parceDate(stringDate: String): Date = {
    val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    new java.sql.Date(dateFormat.parse(stringDate).getTime)
  }

  def parseNginxRecord(record: String): NginxRecord = {
    val datePattern = "\\[+...+\\]".r
    val stringPattern = "\"(.*?)\"".r
    val stringDate = datePattern.findFirstIn(record).get.dropRight(1).drop(1)
    val date = parceDate(stringDate)

    /** All sentence in '"'. First is request data, second is referrer url and the third is user agent. Regex never was
      * my strong side.
      */
    val strings = stringPattern.findAllMatchIn(record).toList.map(_.toString.drop(1).dropRight(1))

    //Now we remove all regexped stuff from or record.
    val filteredRecord = record.replaceAll(datePattern.toString(), "").replaceAll(stringPattern.toString(), "")
    //Split our record and filter empty string.
    val nonStringRecordData = filteredRecord.split(" ").filterNot(_ == "")
    val ip = nonStringRecordData(0)

    //Map data to option. If data is '-' then it's None. Else Some(data).
    val requestData = parseRequestData(strings.head)
    val referrerUrl = if (strings(1) == "-") None else Some(strings(1))
    val userAgent = if (strings(2) == "-") None else Some(strings(2))
    val remoteLog = if (nonStringRecordData(1) == "-") None else Some(nonStringRecordData(1))
    val username = if (nonStringRecordData(2) == "-") None else Some(nonStringRecordData(2))
    val responseCode = if (nonStringRecordData(3) == "-") None else Some(nonStringRecordData(3).toInt)
    val responseSize = if (nonStringRecordData(4) == "-") None else Some(nonStringRecordData(4).toLong)
    NginxRecord(ip,
      remoteLog,
      username,
      date,
      requestData,
      responseCode,
      responseSize,
      referrerUrl,
      userAgent)
  }

  case class NginxRecordLite(ipAddress: String,
                             date: Date,
                             requestData: RequestData,
                             responseCode: Option[Int],
                             userAgent: Option[String])

  case class NginxRecord(ipAddress: String,
                         remoteLog: Option[String],
                         username: Option[String],
                         date: Date,
                         requestData: RequestData,
                         responseCode: Option[Int], //not sure what nginx logging while there is no response
                         responseSize: Option[Long],
                         referrerUrl: Option[String],
                         userAgent: Option[String])

  case class RequestData(requestType: String, requestUrl: String, protocol: String)

}
