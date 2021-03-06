package br.uff.spark

object TransformationType extends Enumeration {

  type TransformationType = Value
  val UNKNOWN,
  MAP,
  FLAT_MAP,
  UNION,
  FILTER,
  REDUCE_BY_KEY,
  DISTINCT,
  INTERSECTION,
  CARTESIAN,
  GROUP_BY_KEY,
  MAP_TO_PAIR,
  PIPE,
  JOIN,
  SUBTRACT,
  CO_GROUPED,
  LEFT_OUTER_JOIN,
  AGGREGATE_BY_KEY,
  FULL_OUTER_JOIN,
  RIGHT_OUTER_JOIN,
  MAP_PARTITIONS_WITH_INDEX,
  ZIP,
  ZIPPED_PARTITIONS_BASE_RDD,
  MAP_VALUES,
  SHUFFLED,
  SORT_BY_KEY,
  GLOM,
  RANGE,
  COALESCED,
  FILE_GROUP = Value
}
