package com.bbva.datioamproduct.fdevdatio.pokemon

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types._

package object fields {

  trait Field {
    val name:String
    lazy val column:Column = col(name)
  }

  case object Generation extends Field {
    override val name:String = "pkmn_generation_number"

    def filtter(date:String): Column = column === date
  }

  case object PokemonName extends Field{
    override val name:String = "pkmn_name"
  }

  case object PokemonNameValid extends Field{
    override val name:String = "pkmn_name_valid"
  }

  case object Ventaja extends Field{
    override val name:String = "Ventaja"
  }

  case object Legendary extends Field{
    override val name:String = "pkmn_legendary_mark_type"
  }

  case object Region extends Field{
    override val name:String = "pkmn_region_name"
  }
  case object Total extends Field{
    override val name:String = "battle_total_number"
  }

  case object Ranked extends Field{
    override val name:String = "ranked"
  }

  case object Rank extends Field{
    override val name:String = "rank"
  }

  case object Nature extends Field{
    override val name:String = "pkmn_nature_type"
  }

  case object Nature1 extends Field{
    override val name:String = "pkmn_nature1_type"
  }

  case object NatureValid extends Field{
    override val name:String = "pkmn_nature_type_valid"
  }

  case object Nature1Valid extends Field{
    override val name:String = "pkmn_nature1_type_valid"
  }

  case object BattleAdvType extends Field{
    override val name:String = "pkmn_battle_advant_type"
  }

  case object BattleAdv1Type extends Field{
    override val name:String = "pkmn_battle_advant1_type"
  }

  case object BattleAdv2Type extends Field{
    override val name:String = "pkmn_battle_advant2_type"
  }

  case object BattleAdv3Type extends Field{
    override val name:String = "pkmn_battle_advant3_type"
  }

  case object BattleAdv4Type extends Field{
    override val name:String = "pkmn_battle_advant4_type"
  }

  case object AdvantageAux extends Field{
    override val name:String = "advantage_aux"
  }

  case object Advantage extends Field{
    override val name:String = "advantage"
  }

  case object BattleAdvTypeColl extends Field{
    override val name:String = "pkmn_battle_advant_type_coll"
  }

  ///////////
  case object BattleDisAdvType extends Field{
    override val name:String = "pkmn_battle_disadv_type"
  }

  case object BattleDisAdv1Type extends Field{
    override val name:String = "pkmn_battle_disadv1_type"
  }

  case object BattleDisAdv2Type extends Field{
    override val name:String = "pkmn_battle_disadv2_type"
  }

  case object BattleDisAdv3Type extends Field{
    override val name:String = "pkmn_battle_disadv3_type"
  }

  case object BattleDisAdv4Type extends Field{
    override val name:String = "pkmn_battle_disadv4_type"
  }

  case object DisadvantageAux extends Field{
    override val name:String = "disadvantage_aux"
  }

  case object Disadvantage extends Field{
    override val name:String = "disadvantage"
  }

  case object BattleDisAdvTypeColl extends Field{
    override val name:String = "pkmn_battle_disadvant_type_coll"
  }

  case object DisadvantageValid extends Field{
    override val name:String = "disadvantage_valid"
  }

  case object BattleAdvTypeValid extends Field{
    override val name:String = "pkmn_battle_advant_type_valid"
  }

  case object BattleAdv1TypeValid extends Field{
    override val name:String = "pkmn_battle_advant1_type_valid"
  }

  case object BattleAdv2TypeValid extends Field{
    override val name:String = "pkmn_battle_advant2_type_valid"
  }

  case object BattleAdv3TypeValid extends Field{
    override val name:String = "pkmn_battle_advant3_type_valid"
  }

  case object BattleAdv4TypeValid extends Field{
    override val name:String = "pkmn_battle_advant4_type_valid"
  }

  case object TotalValid extends Field{
    override val name:String = "battle_total_number_valid"
  }

  case object Ganar extends Field{
    override val name:String = "% de ganar"
  }

  case object GanarTipo extends Field{
    override val name:String = "% de ganar por tipo"
  }

  case object Alan extends Field{
    override val name:String = "Alan"
  }

  case object AlanType1 extends Field{
    override val name:String = "AlanType1"
  }

  case object AlanType2 extends Field{
    override val name:String = "AlanType2"
  }

  case object AlanTotal extends Field{
    override val name:String = "AlanTotal"
  }

  case object Adv extends Field{
    override val name:String = "Adv"
  }

  case object AdvType1 extends Field{
    override val name:String = "AdvType1"
  }

  case object AdvType2 extends Field{
    override val name:String = "AdvType2"
  }

  case object AdvTotal extends Field{
    override val name:String = "AdvTotal"
  }

  case object TotalFinal extends Field{
    override val name:String = "total_final"
  }

  case object AvgGanar extends Field{
    override val name:String = "avg de ganar"
  }
  /*case object ZScore extends Field {
    override val name: String = "z_score"

    def apply(): Column={

      val window: WindowSpec = Window.partitionBy(NationalityName.column, CatAge())

      ((Overall.column - mean(Overall.column).over(window))/stddev(Overall.column).over(window)).alias(name)
      //mean(Overall.column).over(window).alias(name)
    }
  }*/

}
