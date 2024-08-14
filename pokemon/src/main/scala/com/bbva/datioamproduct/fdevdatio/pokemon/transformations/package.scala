package com.bbva.datioamproduct.fdevdatio.pokemon

import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.{AlanPokemonTag, Comma, CommaSpace}
import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.JoinTags._
import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.PokemonTypeTag._
import com.bbva.datioamproduct.fdevdatio.pokemon.fields._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.Window

import java.util.Date

package object transformations {

  implicit class Transformations(ds: Dataset[Row]) {

    def addColumn(column: Column): Dataset[Row] = {
      ds.select(
        ds.columns.map(col) :+ column: _*
      )
    }

    def addColumn(listColumn: Seq[Column]): Dataset[Row] = {
      ds.select(
        ds.columns.map(col) ++ listColumn: _*
      )
    }

    def legendary_filter: Dataset[Row] = {
      ds.filter(Legendary.column === "FALSE")
    }

    def topPokemon: Dataset[Row] = {
      val window = Window.partitionBy(Region.column).orderBy(Total.column.desc)
      ds.select(ds.columns.map(col) :+ rank().over(window).alias(Ranked.name) :_*).filter(Ranked.column < 7)
    }

    def joinDF(ventajasTable: Dataset[Row]): Dataset[Row] =
      ds.join(ventajasTable, Seq(Nature.name), InnerTag)

    def cadenaVentajas: Dataset[Row] = {
      ds.select(ds.columns.map(col) :+ when(BattleAdvType.column === NormalTag, NewNormalTag)
        .otherwise(BattleAdvType.column).alias(BattleAdvTypeColl.name) :_*)
        .select(ds.columns.map(col) :+ concat_ws(CommaSpace, BattleAdvTypeColl.column
          ,BattleAdv1Type.column, BattleAdv2Type.column, BattleAdv3Type.column, BattleAdv4Type.column)
          .alias(AdvantageAux.name) :_*)
        .select(ds.columns.map(col) :+ split(AdvantageAux.column, Comma).alias(Advantage.name) :_*)
        .drop(BattleAdvTypeColl.name, AdvantageAux.name)
    }

    def cadenaDesventajas: Dataset[Row] = {
      ds.select(ds.columns.map(col) :+ when(BattleDisAdvType.column === NormalTag, NewNormalTag)
        .otherwise(BattleDisAdvType.column).alias(BattleDisAdvTypeColl.name) :_*)
        .select(ds.columns.map(col) :+ concat_ws(CommaSpace, BattleDisAdvTypeColl.column
          ,BattleDisAdv1Type.column, BattleDisAdv2Type.column, BattleDisAdv3Type.column
          ,BattleDisAdv4Type.column).alias(DisadvantageAux.name) :_*)
        .select(ds.columns.map(col) :+ split(DisadvantageAux.column, Comma)
          .alias(Disadvantage.name) :_*)
        .drop(BattleDisAdvTypeColl.name, DisadvantageAux.name)
    }

    def filterAlanPokemon: Dataset[Row] = {
      ds.filter(PokemonName.column.isin(AlanPokemonTag:_*))
    }

    def filterPokemon: Dataset[Row] = {
      ds.filter(!PokemonName.column.isin(AlanPokemonTag:_*))
    }

    def createPokemonList(ventajas: Dataset[Row]): Dataset[Row] = {
      ds.joinDF(ventajas).cadenaVentajas.cadenaDesventajas
    }

    def crossJoin(alanPokemon: Dataset[Row]): Dataset[Row] = {
      val newValidPokemon = ds.select(ds.columns.map(c => col(c).alias(s"${c}_valid")): _*)
      newValidPokemon.crossJoin(alanPokemon)
    }

  }

}
