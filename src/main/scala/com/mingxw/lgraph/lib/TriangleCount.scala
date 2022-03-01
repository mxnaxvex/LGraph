/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mingxw.lgraph.lib

import com.mingxw.lgraph._

import scala.reflect.ClassTag

/**
  * Compute the number of triangles passing through each vertex.
  *
  * The algorithm is relatively straightforward and can be computed in three steps:
  *
  * <ul>
  * <li> Compute the set of neighbors for each vertex</li>
  * <li> For each edge compute the intersection of the sets and send the count to both vertices.</li>
  * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.</li>
  * </ul>
  *
  * There are two implementations.  The default `TriangleCount.run` implementation first removes
  * self cycles and canonicalizes the graph to ensure that the following conditions hold:
  * <ul>
  * <li> There are no self edges</li>
  * <li> All edges are oriented (src is greater than dst)</li>
  * <li> There are no duplicate edges</li>
  * </ul>
  * However, the canonicalization procedure is costly as it requires repartitioning the graph.
  * If the input data is already in "canonical form" with self cycles removed then the
  * `TriangleCount.runPreCanonicalized` should be used instead.
  *
  * {{{
  * val canonicalGraph = graph.mapEdges(e => 1).removeSelfEdges().canonicalizeEdges()
  * val counts = TriangleCount.runPreCanonicalized(canonicalGraph).vertices
  * }}}
  *
  */
object TriangleCount {
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {

    // Construct set representations of the neighborhoods
    val setGraph = graph.messageProgram[Set[VertexId], Set[VertexId]](
      et => if (et.srcId != et.dstId) Iterator(et.srcId -> Set(et.dstId), et.dstId -> Set(et.srcId)) else Iterator.empty,
      _ ++ _) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[Set[VertexId], ED]): Iterator[(VertexId, Int)] = {
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      } else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      Iterator(et.srcId -> counter, et.dstId -> counter)
    }

    // compute the intersection along edges
    // Merge counters with the graph and divide by two since each triangle is counted twice
    setGraph.messageProgram[Int, Int](edgeFunc, _ + _) {
      (vid, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // double count should be even (divisible by two)
        assert((dblCount & 1) == 0)
        dblCount / 2
    }
  }
}
