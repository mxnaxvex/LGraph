package com.mingxw

package object lgraph {
  /**
    * A 64-bit vertex identifier that uniquely identifies a vertex within a graph. It does not need
    * to follow any ordering or any constraints other than uniqueness.
    */
  type VertexId = Long

  /** Integer identifier of a graph partition. Must be less than 2^30. */
  // TODO: Consider using Char.
  type PartitionID = Int
}
