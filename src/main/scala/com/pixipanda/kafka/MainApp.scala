package  com.pixipanda.kafka

import com.pixipanda.kafka.ConfigParser
import com.pixipanda.kafka.producer.IoTTrafficProducer


object  MainApp {

  def main(args: Array[String]) {

    val configParser = new ConfigParser()
    val kafkaConfig = configParser.loadKafkaConfig
    val iotTrafficProducer = new IoTTrafficProducer(kafkaConfig)
    iotTrafficProducer.generateEvents()
  }
}