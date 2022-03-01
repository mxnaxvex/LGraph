package com.mingxw.lgraph.lib

import com.mingxw.lgraph.{EdgeTriplet, Graph, VertexId}

object KTruss {
  def run(graph: Graph[_, _], maxSteps: Int = Int.MaxValue):
  Graph[Int, Int] = {

    var nbrGraph = graph.removeSelfEdges().messageProgram[(Set[VertexId], Int), Set[VertexId]](
      et => Iterator(et.srcId -> Set(et.dstId), et.dstId -> Set(et.srcId)), _ ++ _) {
      case (vid, _, nbrs) => nbrs.getOrElse(Set.empty[VertexId]) -> 0
    }
      .mapTriplets(et => (et.srcAttr._1.intersect(et.dstAttr._1)).size)

    def sendMsg(k: Int)(et: EdgeTriplet[(Set[VertexId], Int), Int]):
    Iterator[(VertexId, Set[VertexId])] = {
      if (et.attr == k &&
        !et.srcAttr._1.isEmpty && !et.dstAttr._1.isEmpty &&
        (et.srcAttr._1.contains(et.dstId) || et.dstAttr._1.contains(et.srcId))) {
        Iterator(et.srcId -> Set(et.dstId), et.dstId -> Set(et.srcId))
      }
      else
        Iterator.empty
    }

    var k = 0
    var num = nbrGraph.edges.filter(edge => edge.attr > k).length
    while (num > 0 && k < maxSteps) {
      var actives = nbrGraph.triplets.count(edgeTriplet => edgeTriplet.attr <= k &&
        !edgeTriplet.srcAttr._1.isEmpty && !edgeTriplet.dstAttr._1.isEmpty
        && (edgeTriplet.srcAttr._1.contains(edgeTriplet.dstId) ||
        edgeTriplet.dstAttr._1.contains(edgeTriplet.srcId)))
      while (actives > 0) {
        nbrGraph = nbrGraph.messageProgram[(Set[VertexId], Int), Set[VertexId]](sendMsg(k), _ ++ _) {
          (vid, vd, msg) =>
            msg match {
              case Some(m) =>
                if (vd._1.diff(m).isEmpty)
                  Set.empty[VertexId] -> k
                else
                  vd._1.diff(m) -> vd._2
              case None => vd._1 -> vd._2
            }
        }
        nbrGraph = nbrGraph.mapTriplets(et => (et.srcAttr._1.intersect(et.dstAttr._1)).size)
        actives = nbrGraph.triplets.count(edgeTriplet => edgeTriplet.attr == k &&
          !edgeTriplet.srcAttr._1.isEmpty && !edgeTriplet.dstAttr._1.isEmpty
          && (edgeTriplet.srcAttr._1.contains(edgeTriplet.dstId) ||
          edgeTriplet.dstAttr._1.contains(edgeTriplet.srcId)))
      }
      nbrGraph = nbrGraph.mapTriplets(et => (et.srcAttr._1.intersect(et.dstAttr._1)).size)
      num = nbrGraph.edges.filter(edge => edge.attr > k).length
      k += 1
    }
    nbrGraph.mapVertices((vid, vd) => (vd._2 + 2))
  }
}
