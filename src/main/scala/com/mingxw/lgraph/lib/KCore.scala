package com.mingxw.lgraph.lib

import com.mingxw.lgraph.{EdgeTriplet, Graph, VertexId}

object KCore {
  def run(graph: Graph[_, Double], maxSteps: Int = Int.MaxValue): Graph[Int, Double] = {

    var kcGraph = graph.messageProgram[(Double, Boolean), Double](
      et => Iterator(et.srcId -> et.attr, et.dstId -> et.attr), _ + _)(
      (vid, vd, msg) => {
        val d = msg.getOrElse(0.0)
        if (d <= 1.0) (1.0 -> false)
        else (d -> false)
      }
    )

    def sendMsg(k: Int)(et: EdgeTriplet[(Double, Boolean), Double]): Iterator[(VertexId, Double)] = {
      //2端都非final，且一端为k，另一端大于k
      if (!et.srcAttr._2 && !et.dstAttr._2 &&
        math.abs(et.srcAttr._1 - k) < 1e-6 && et.dstAttr._1 > k) {
        Iterator(et.dstId -> et.attr)
      } else if (!et.srcAttr._2 && !et.dstAttr._2 &&
        math.abs(et.dstAttr._1 - k) < 1e-6 && et.srcAttr._1 > k) {
        Iterator(et.srcId -> et.attr)
      } else {
        Iterator.empty
      }
    }

    var k = 1
    var num = kcGraph.vertices.filter(x => x._2._1 > k).length
    while (num > 0 && k <= maxSteps) {
      // kcore == k 且 非final的点需要发送消息更新对端
      var actives = kcGraph.vertices.filter(x => x._2._1 == k && !x._2._2).length
      while (actives > 0) {
        kcGraph = kcGraph.messageProgram[(Double, Boolean), Double](sendMsg(k), _ + _) {
          case (vid, (kcore, isFinal), msg) =>
            msg match {
              case Some(m) =>
                //kcore - m <= k 说明此顶点属于k core, isFinal设置为false，下一轮需要更新对端点
                if (kcore - m <= k) k.toDouble -> false
                else kcore - m -> false
              case None =>
                // isFinal==true的点属性不变，kcore == k的点isFinal设置为true，下一轮不在发送消息出去
                if (isFinal || kcore == k) kcore -> true
                else kcore -> isFinal
            }
        }
        actives = kcGraph.vertices.filter(x => x._2._1 == k && !x._2._2).length
      }

      num = kcGraph.vertices.filter(x => x._2._1 > k).length
      k += 1
    }
    kcGraph.mapVertices((vid, vd) => vd._1.toInt)
  }

}
