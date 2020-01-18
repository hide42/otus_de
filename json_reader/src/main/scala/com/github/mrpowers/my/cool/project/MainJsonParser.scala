package com.github.mrpowers.my.cool.project

import org.json4s._
import org.json4s.jackson.JsonMethods._

object MainJsonParser extends SparkSessionWrapper with App{

  case class Wine(
                   id: Option[BigInt], country: Option[String],
                   points: Option[BigInt], title: Option[String],
                   variety: Option[String], winery: Option[String]
                 )

  implicit val jsonFormats: Formats = DefaultFormats
  spark
    .sparkContext.textFile(args(0))
    .map(record => parse(record).extract[Wine])
    .foreach(wine => println(
      wine.id.getOrElse("UNKNOWN"),
      wine.country.getOrElse("UNKNOWN"),
      wine.points.getOrElse("UNKNOWN"),
      wine.title.getOrElse("EMPTY"),
      wine.variety.getOrElse("UNKNOWN"),
      wine.winery.getOrElse("UNKNOWN")
    ))
}
