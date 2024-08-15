package com.bbva.datioamproduct.fdevdatio.pokemon

import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.{AlanPokemonTag, Comma, CommaSpace, FalseTag, NadieTag}
import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.JoinTags._
import com.bbva.datioamproduct.fdevdatio.pokemon.common.ConfigConstants.PokemonTypeTag._
import com.bbva.datioamproduct.fdevdatio.pokemon.fields._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import com.typesafe.config.Config
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.FloatType

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

    def regionCount: Dataset[Row] =
      ds.groupBy(Region.column).agg(count(Region.column))

    def legendaryFilter: Dataset[Row] = {
      ds.filter(Legendary.column === FalseTag)
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

    def crossJoinPokemon(alanPokemon: Dataset[Row]): Dataset[Row] = {
      ds.select(ds.columns.map(c => col(c).alias(s"${c}_valid")): _*)
        .crossJoin(alanPokemon)

    }
    //
    def battleTest: Dataset[Row] = {

      val baseColumn = col("battle_total_number_valid")
      val advantageColumns = Seq(
        BattleAdvType.name,
        BattleAdv1Type.name,
        BattleAdv2Type.name,
        BattleAdv3Type.name,
        BattleAdv4Type.name
      )

      val validAdvantageColumns = Seq(
        BattleAdvTypeValid.name,
        BattleAdv1TypeValid.name,
        BattleAdv2TypeValid.name,
        BattleAdv3TypeValid.name,
        BattleAdv4TypeValid.name
      )

      val totalSum = validAdvantageColumns.foldLeft(baseColumn) { (accum, colName) =>
        accum + when(array_contains(Disadvantage.column, col(colName)), 50).otherwise(0)
      }

      val totalFinal = advantageColumns.foldLeft(totalSum) { (accum, colName) =>
        accum - when(array_contains(DisadvantageValid.column, col(colName)), 50).otherwise(0)
      }

      ds.select(ds.columns.map(col) :+ totalFinal.alias(TotalFinal.name): _*)
    }

    def battleProbability: Dataset[Row] = {

      val percentageDf = ds.select(ds.columns.map(col) :+
          format_number(TotalValid.column / Total.column * 100, 2).cast(FloatType).alias(Ganar.name) :+
          format_number(TotalFinal.column / Total.column * 100, 2).cast(FloatType).alias(GanarTipo.name) :_*)
        .select(ds.columns.map(col) :+ Ganar.column :+ GanarTipo.column :+
          when(Ganar.column > GanarTipo.column, PokemonName.column)
            .when(Ganar.column < GanarTipo.column, PokemonNameValid.column)
            .otherwise(lit(NadieTag)).alias(Ventaja.name) :_*)

      val avgGanar = percentageDf.groupBy(PokemonNameValid.column).agg(format_number(avg(GanarTipo.column), 2)
          .cast(FloatType).alias(AvgGanar.name))
        .select(PokemonNameValid.column, AvgGanar.column)

      val joinPercentageAvg = percentageDf.join(avgGanar, Seq(PokemonNameValid.name), InnerTag)

      joinPercentageAvg.select(PokemonName.column.alias(Alan.name),Nature.column.alias(AlanType1.name),
        col(Nature1.name).alias(AlanType2.name), Total.column.alias(AlanTotal.name),
        Ventaja.column, PokemonNameValid.column.alias(Adv.name), NatureValid.column.alias(AdvType1.name),
        Nature1Valid.column.alias(AdvType2.name), TotalValid.column.alias(AdvTotal.name),
        Ganar.column.alias(Ganar.name), GanarTipo.column, AvgGanar.column)

    }

    def theVeryBest: Dataset[Row] = {

      val window = Window.partitionBy(Alan.column).orderBy(Ganar.column.desc)
      ds.filter(Ganar.column < GanarTipo.column)
        .select(ds.columns.map(col) :+ rank().over(window).alias(Rank.name) :_*)
        .filter(Rank.column < 7)
        .drop(Rank.name)
    }
  }

}
