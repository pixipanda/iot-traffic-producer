package com.pixipanda.kafka



import com.typesafe.config._
import org.apache.log4j.Logger

/**
 * Created by kafka on 11/11/18.
 */
class ConfigParser {

  val logger = Logger.getLogger(getClass.getName)

  def loadKafkaConfig: Config = {
    try {
      // load the generator.conf file
      val config = ConfigFactory.load("iotproducer")
      val kafkaConfig = config.getConfig("iottraffic.producer")
      // validate the configuration against reference configuration file
      config.checkValid(ConfigFactory.defaultReference(), "iottraffic.producer")
      kafkaConfig

    } catch {
      case e: ConfigException => throw new RuntimeException(s"Configuration validation failed!: $e")
    }

  }

  def printSystemProperties() {

    val p = System.getProperties
    val keys = p.keys

    while (keys.hasMoreElements) {
      val k = keys.nextElement
      val v = p.get(k)
      logger.debug(k + ": " + v)
    }
  }

}