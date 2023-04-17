package com.gishorizon.reader

import play.api.libs.json.{JsObject, JsValue, Json}

import java.net.{HttpURLConnection, URL}
import scala.io.Source

object HttpUtils {

  def getRequest(callback: (Boolean, JsValue) => Unit): Unit = {
    val url = new URL("http://localhost:8082/getDataRefForAoi/?sensorName=Landsat_OLI&tIndex=914925604&level=12&aoiCode=qwertyuiopasdfgh")
    val connection = url.openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    val responseCode = connection.getResponseCode
    if (responseCode == HttpURLConnection.HTTP_OK) {
      val inputStream = connection.getInputStream
      val responseBody = Source.fromInputStream(inputStream).mkString
      inputStream.close()
      val json: JsValue = Json.parse(responseBody)
      callback(true, json)
//      if (json.asInstanceOf[JsObject].value("error").toString() == "false") {
//
//      } else {
//
//      }
    } else {
      callback(false, null)
    }
    connection.disconnect()
  }



}
