package com.bbva.datioamproduct.fdevdatio.pokemon

import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.InputTag
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.collection.convert.ImplicitConversions.`set asScala`
import com.bbva.datioamproduct.fdevdatio.pokemon.fields.Field

import scala.language.implicitConversions

package object utils {
  case class Params(devName: String, fifaUpdateDate: String)

  implicit class PokemonConfig(config:Config) extends IOUtils {

    def readInputs: Map[String, Dataset[Row]] = {
      config.getObject(InputTag).keySet()
        .map(key=>{
          (key, read(config.getConfig(s"$InputTag.$key")))
        }).toMap
    }


    def getParams: Params = Params(
      devName = config.getString("DevNameTag"),
      fifaUpdateDate = config.getString("FifaUpdateDate")
    )

  }

}
