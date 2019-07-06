package com.pixipanda.kafka.model

import java.util.Date


case class IoTData(vehicleId:String, vehicleType:String, routeId:String, longitude:String, latitude:String, timeStamp:Date, speed:Double, fuelLevel:Double)

