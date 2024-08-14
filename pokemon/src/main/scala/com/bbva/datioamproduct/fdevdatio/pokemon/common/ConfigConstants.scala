package com.bbva.datioamproduct.fdevdatio.pokemon.common

object ConfigConstants {

  val RootTag: String = "pokemonJob"
  val InputTag: String = s"$RootTag.input"
  val CommaSpace: String = ", "
  val Comma: String = ","


  case object Tags {
    val PokemonTag: String = "pokemon"
    val VentajasTag: String = "ventajas"
  }

  case object JoinTags {
    val InnerTag: String = "inner"
    val OuterTag: String = "outer"
    val LeftTag: String = "left"
    val RightTag: String = "right"
    val LeftAntiTag: String = "left_anti"
    val LeftSemiTag: String = "left_semi"
  }

  case object PokemonTypeTag {
    val NormalTag: String = "Normal"
    val NewNormalTag: String = "(*)"
  }

  val AlanPokemonTag: Seq[String] = Seq("CharizardMega Charizard X","BlastoiseMega Blastoise",
    "ScizorMega Scizor","LucarioMega Lucario","GengarMega Gengar","TyranitarMega Tyranitar")

  val Options: String = "options"
  val OverrideSchema: String = s"$Options.overrideSchema"
  val MergeSchema: String = s"$Options.mergeSchema"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"
  val CoalesceNumber: String = s"$Options.coalesce"
  val Delimiter: String = s"$Options.delimiter"
  val Header: String = s"$Options.header"

  val Schema: String = "schema"
  val SchemaPath: String = s"$Schema.path"
  val IncludeMetadataFields: String = s"$Schema.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Schema.includeDeletedFields"


  val Path: String = "path"
  val Table: String = "table"
  val Type: String = "type"

  val PartitionOverwriteModeString: String = s"$Options.partitionOverwriteMode"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

  val DelimiterOption: String = "delimiter"
  val HeaderOption: String = "header"
  val OverrideSchemaOption: String = "overrideSchema"
  val MergeSchemaOption: String = "mergeSchema"

}
