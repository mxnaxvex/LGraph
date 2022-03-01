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

package com.mingxw.lgraph

import com.mingxw.lgraph.impl.GraphImpl

import scala.collection.mutable.BitSet
import scala.reflect.ClassTag

/**
  * Implements a Pregel-like bulk-synchronous message-passing API.
  *
  * Unlike the original Pregel API, the GraphX Pregel API factors the sendMessage computation over
  * edges, enables the message sending computation to read both vertex attributes, and constrains
  * messages to the graph structure.  These changes allow for substantially more efficient
  * distributed execution while also exposing greater flexibility for graph-based computation.
  *
  * @example We can use the Pregel abstraction to implement PageRank:
  *    {{{
  *         val pagerankGraph: Graph[Double, Double] = graph
  *         // Associate the degree with each vertex
  *         .outerJoinVertices(graph.outDegrees) {
  *            (vid, vdata, deg) => deg.getOrElse(0)
  *          }
  *          // Set the weight on the edges based on the degree
  *          .mapTriplets(e => 1.0 / e.srcAttr)
  *          // Set the vertex attributes to the initial pagerank values
  *          .mapVertices((id, attr) => 1.0)
  *
  *        def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
  *          resetProb + (1.0 - resetProb) * msgSum
  *        def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
  *          Iterator((edge.dstId, edge.srcAttr * edge.attr))
  *        def messageCombiner(a: Double, b: Double): Double = a + b
  *        val initialMessage = 0.0
  *        // Execute Pregel for a fixed number of iterations.
  *        Pregel(pagerankGraph, initialMessage, numIter)(
  *          vertexProgram, sendMessage, messageCombiner)
  *     }}}
  *
  */
object Pregel {

  /**
    * Execute a Pregel-like iterative vertex-parallel abstraction.  The
    * user-defined vertex-program `vprog` is executed in parallel on
    * each vertex receiving any inbound messages and computing a new
    * value for the vertex.  The `sendMsg` function is then invoked on
    * all out-edges and is used to compute an optional message to the
    * destination vertex. The `mergeMsg` function is a commutative
    * associative function used to combine messages destined to the
    * same vertex.
    *
    * On the first iteration all vertices receive the `initialMsg` and
    * on subsequent iterations if a vertex does not receive a message
    * then the vertex-program is not invoked.
    *
    * This function iterates until there are no remaining messages, or
    * for `maxIterations` iterations.
    *
    * @tparam VD the vertex data type
    * @tparam ED the edge data type
    * @tparam A  the Pregel message type
    * @param graph           the input graph.
    * @param initialMsg      the message each vertex will receive at the first
    *                        iteration
    * @param maxIterations   the maximum number of iterations to run for
    * @param activeDirection the direction of edges incident to a vertex that received a message in
    *                        the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
    *                        out-edges of vertices that received a message in the previous round will run. The default is
    *                        `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
    *                        in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
    *                        *both* vertices received a message.
    * @param vprog           the user-defined vertex program which runs on each
    *                        vertex and receives the inbound message and computes a new vertex
    * value.  On the first iteration the vertex program is invoked on
    *                        all vertices and is passed the default message.  On subsequent
    *                        iterations the vertex program is only invoked on those vertices
    *                        that receive messages.
    * @param sendMsg         a user supplied function that is applied to out
    *                        edges of vertices that received messages in the current
    *                        iteration
    * @param mergeMsg        a user supplied function that takes two incoming
    *                        messages of type A and merges them into a single message of type
    * A.  ''This function must be commutative and associative and
    *                        ideally the size of A should not increase.''
    * @return the resulting graph at the end of the computation
    *
    */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val vertices = graph.vertices.map(x => x._1 -> vprog(x._1, x._2, initialMsg))
    val global2Local = vertices.map(_._1).zipWithIndex.toMap
    val edges = graph.edges
    val localIds = edges.map(x => global2Local.get(x.srcId).get -> global2Local.get(x.dstId).get)
    val msgs = new Array[A](vertices.length)
    var bitset: BitSet = null

    var i = 0
    while ((bitset == null || bitset.size > 0) && i < maxIterations) {
      val newBitset = new BitSet(vertices.length)
      genTripletIterator(vertices, edges, localIds, bitset, activeDirection)
        .flatMap(sendMsg)
        .foreach { case (vid, msg) =>
          val lid = global2Local.getOrElse(vid, -1)
          if (lid >= 0) {
            if (newBitset(lid)) {
              msgs(lid) = mergeMsg(msgs(lid), msg)
            } else {
              msgs(lid) = msg
              newBitset.add(lid)
            }
          }
        }
      newBitset.foreach(lid => {
        val v = vertices(lid)
        vertices(lid) = v._1 -> vprog(v._1, v._2, msgs(lid))
      })
      bitset = newBitset
      i += 1
    }
    GraphImpl[VD, ED](vertices, edges)

  } // end of apply

  private def genTripletIterator[VD, ED](vertices: Array[(VertexId, VD)],
                                         edges: Array[Edge[ED]],
                                         localIds: Array[(Int, Int)],
                                         bitset: BitSet,
                                         activeDirection: EdgeDirection): Iterator[EdgeTriplet[VD, ED]] = {
    genEdgeIterator(localIds, bitset, activeDirection).map(i => {
      val et = new EdgeTriplet[VD, ED]()
      et.srcId = edges(i).srcId
      et.dstId = edges(i).dstId
      et.attr = edges(i).attr
      et.srcAttr = vertices(localIds(i)._1)._2
      et.dstAttr = vertices(localIds(i)._2)._2
      et
    })
  }

  private def genEdgeIterator(localIds: Array[(Int, Int)], bitset: BitSet, activeDirection: EdgeDirection): Iterator[Int] = {
    val numEdge = localIds.length
    val ite = Iterator.range(0, numEdge)
    if (bitset == null) return ite
    if (bitset.size == 0) return Iterator.empty
    activeDirection match {
      case EdgeDirection.Either =>
        ite.filter(e => bitset(localIds(e)._1) || bitset(localIds(e)._2))
      case EdgeDirection.Both =>
        ite.filter(e => bitset(localIds(e)._1) && bitset(localIds(e)._2))
      case EdgeDirection.In =>
        ite.filter(e => bitset(localIds(e)._2))
      case EdgeDirection.Out =>
        ite.filter(e => bitset(localIds(e)._1))
    }
  }

} // end of class Pregel
