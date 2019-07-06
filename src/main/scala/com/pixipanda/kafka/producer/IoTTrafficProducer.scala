package com.pixipanda.kafka.producer

import java.util.{Collections, Date, UUID, Properties, ArrayList}

import com.google.gson
import com.google.gson.{Gson, JsonObject}
import com.pixipanda.kafka.model.IoTData
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

import scala.util.Random


class  IoTTrafficProducer(config:Config) {


  val prop = createProducerConfig()
  val producer = new KafkaProducer[String, String](prop)
  val topic = config.getString("topic")


  def createProducerConfig() = {
    val props = new Properties()
    props.put("bootstrap.servers", config.getString("bootstrap.servers"))
    props.put("acks", config.getString("acks"))
    props.put("retries", config.getString("retries"))
    props.put("batch.size", config.getString("batch.size"))
    props.put("linger.ms", config.getString("linger.ms"))
    props.put("buffer.memory", config.getString("buffer.memory"))
    props.put("key.serializer", config.getString("key.serializer"))
    props.put("value.serializer", config.getString("value.serializer"))
    //props.put("schema.registry.url", config.getString("schema.registry.url"))
    //props.put("auto.register.schemas",config.getString("auto.register.schemas"))

    props
  }


  def generateEvents() = {
  val routeList = List("Route-37", "Route-43", "Route-82")
  val vehicleTypeList = List("Large Truck", "Small Truck", "Private Car", "Bus", "Taxi")
  val rand: Random = new Random
  val gson: Gson = new Gson

    //logger.info("Sending events")
  // generate event in loop
  while (true) {
    val eventList = new ArrayList[IoTData]
      for (i <- 0 to 100) {
          val vehicleId: String = UUID.randomUUID.toString
          val vehicleType: String = vehicleTypeList(rand.nextInt(5))
          val routeId: String = routeList(rand.nextInt(3))
          val timestamp: Date = new Date
          val speed: Double = rand.nextInt(100 - 20) + 20
          val fuelLevel: Double = rand.nextInt(40 - 10) + 10

          for (i <- 0 to 5) {
            val coords: String = getCoordinates(routeId)
            val latitude: String = coords.substring(0, coords.indexOf(","))
            val longitude: String = coords.substring(coords.indexOf(",") + 1, coords.length)
            val event: IoTData = new IoTData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed, fuelLevel)
           // println("vehicleId: " + vehicleId + " event vehicleId: " + event.vehicleId)
            eventList.add(event)
          }
        }
    Collections.shuffle(eventList)
    import scala.collection.JavaConversions._
    for (event <- eventList) {

      val eventJsonObj: JsonObject = new JsonObject
      eventJsonObj.addProperty("vehicleId", event.vehicleId)
      eventJsonObj.addProperty("vehicleType", event.vehicleType)
      eventJsonObj.addProperty("routeId", event.routeId)
      eventJsonObj.addProperty("latitude", event.latitude)
      eventJsonObj.addProperty("longitude", event.longitude)
      eventJsonObj.addProperty("time", new Date().toString)
      eventJsonObj.addProperty("speed", event.speed)
      eventJsonObj.addProperty("fuelLevel", event.fuelLevel)

      val eventJsonString = gson.toJson(eventJsonObj)
      val producerRecord = new ProducerRecord[String, String](topic, eventJsonString)

      producer.send(producerRecord)
      println("Traffic Event: " + eventJsonString)
      Thread.sleep(rand.nextInt(3000 - 1000) + 1000)
    }
  }
}

  def getCoordinates(routeId:String) = {
    val rand: Random = new Random
    var latPrefix: Int = 0
    var longPrefix: Int = -0
    if (routeId == "Route-37") {
      latPrefix = 33
      longPrefix = -96
    }
    else if (routeId == "Route-82") {
      latPrefix = 34
      longPrefix = -97
    }
    else if (routeId == "Route-43") {
      latPrefix = 35
      longPrefix = -98
    }
    val lati: Float = latPrefix + rand.nextFloat
    val longi: Float = longPrefix + rand.nextFloat
    lati + "," + longi
  }
}