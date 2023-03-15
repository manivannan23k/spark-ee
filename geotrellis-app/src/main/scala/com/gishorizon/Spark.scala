package com.gishorizon

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  def conf: SparkConf = new SparkConf()
    .setIfMissing("spark.master", "local[*]")
//    .setIfMissing("spark.master", "spark://192.168.137.1:7077")
//    .setIfMissing("spark.driver.bindAddress","127.0.0.1")
//    .setIfMissing("spark.master", "192.168.14.41[*]")
//    .setIfMissing("spark.master", "192.168.102.2[*]")
    .setAppName("spark-ee")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "128m")
    .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
//    .set("spark.driver.memory", "5g")
//    .set("spark.executionEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))

  implicit val session: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
  implicit def context: SparkContext = session.sparkContext
}
