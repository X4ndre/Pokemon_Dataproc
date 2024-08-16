package com.bbva.datioamproduct.fdevdatio.pokemon

import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.Tags._
import com.bbva.datioamproduct.fdevdatio.pokemon.transformations.Transformations
import com.bbva.datioamproduct.fdevdatio.pokemon.utils.{IOUtils, PokemonConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

class PokemonJob extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "PokemonJob"

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    val config: Config = runtimeContext.getConfig

    val mapDs: Map[String, Dataset[Row]] = config.readInputs

    val pokemonDf: Dataset[Row] = mapDs(PokemonTag)
    val ventajasDf: Dataset[Row] = mapDs(VentajasTag)

    //pokemonDf.regionCount.show()

    //3.1 Filtro Legendarios
    //pokemonDf.legendaryFilter.show()

    //3.2 Top 6 por región
    //pokemonDf.legendaryFilter.topPokemon.show()

    //3.3 Join
    //pokemonDf.legendaryFilter.joinDF(ventajasDf).show()

    //3.4-3.5 Cadena de ventajas y desventajas
    //pokemonDf.joinDF(ventajasDf).cadenaVentajas.cadenaDesventajas.show()

    //3.6 Función Objetivo
    val alanPokemon: Dataset[Row] = pokemonDf.filterAlanPokemon.createPokemonList(ventajasDf)
    val validPokemon: Dataset[Row] = pokemonDf.filterPokemon.createPokemonList(ventajasDf)

    val result = validPokemon.crossJoinPokemon(alanPokemon).battleTest
    result.battleProbability.theVeryBest.show()

    //3.7
    0
  }

}
