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
  case object Nature extends Field{
    override val name:String = "pkmn_nature_type"
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
  /*case object ZScore extends Field {
    override val name: String = "z_score"

    def apply(): Column={

      val window: WindowSpec = Window.partitionBy(NationalityName.column, CatAge())

      ((Overall.column - mean(Overall.column).over(window))/stddev(Overall.column).over(window)).alias(name)
      //mean(Overall.column).over(window).alias(name)
    }
  }*/

}
