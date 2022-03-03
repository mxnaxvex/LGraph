package com.mingxw.lgraph.lib

import com.mingxw.lgraph.{EdgeTriplet, Graph, VertexId}

object KTruss {
  def run(graph: Graph[_, _], maxSteps: Int = Int.MaxValue): Graph[Int, Int] = {

    //计算每条边包含三角形的数量，又称支持度；顶点存储所有对端的顶点Id，初始支持度为0
    var nbrGraph = graph.removeSelfEdges().messageProgram[(Set[VertexId], Int), Set[VertexId]](
      et => Iterator(et.srcId -> Set(et.dstId), et.dstId -> Set(et.srcId)), _ ++ _) {
      case (vid, _, nbrs) => nbrs.getOrElse(Set.empty[VertexId]) -> 0
    }
      .mapTriplets(et => (et.srcAttr._1.intersect(et.dstAttr._1)).size)

    def sendMsg(k: Int)(et: EdgeTriplet[(Set[VertexId], Int), Int]):
    Iterator[(VertexId, Set[VertexId])] = {
      //若边的支持度<=k，srcAttr包含dstId且dstAttr包含srcId，说明这条条因数据变动后确定属于k-truss，两端的顶点删除对方对方的Id
      //支持度<=k但顶点不包含对方点id的，说明是之前清除过，无需再次发消息更新
      if (et.attr <= k &&
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
      //边的支持度<=k，且srcAttr包含dstId，dstAttr包含srcId，这些边两端的顶点需更新Id集
      var actives = nbrGraph.triplets.count(edgeTriplet => edgeTriplet.attr <= k &&
        !edgeTriplet.srcAttr._1.isEmpty && !edgeTriplet.dstAttr._1.isEmpty
        && (edgeTriplet.srcAttr._1.contains(edgeTriplet.dstId) ||
        edgeTriplet.dstAttr._1.contains(edgeTriplet.srcId)))

      while (actives > 0) {
        nbrGraph = nbrGraph.messageProgram[(Set[VertexId], Int), Set[VertexId]](sendMsg(k), _ ++ _) {
          (vid, vd, msg) =>
            msg match {
              case Some(m) =>
                val d = vd._1.diff(m)
                //若删除后顶点集为空，则说明该顶点属于k-truss
                if (d.isEmpty)
                  Set.empty[VertexId] -> k
                else
                  d -> vd._2
              case None => vd._1 -> vd._2
            }
        }
        //更新边的支持度
        nbrGraph = nbrGraph.mapTriplets(et => (et.srcAttr._1.intersect(et.dstAttr._1)).size)

        actives = nbrGraph.triplets.count(edgeTriplet => edgeTriplet.attr <= k &&
          !edgeTriplet.srcAttr._1.isEmpty && !edgeTriplet.dstAttr._1.isEmpty
          && (edgeTriplet.srcAttr._1.contains(edgeTriplet.dstId) ||
          edgeTriplet.dstAttr._1.contains(edgeTriplet.srcId)))
      }

      num = nbrGraph.edges.filter(edge => edge.attr > k).length
      k += 1
    }
    nbrGraph.mapVertices((vid, vd) => (vd._2 + 2))
  }
}
