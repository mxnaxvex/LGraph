package com.mingxw.lgraph.lib

import com.mingxw.lgraph.{EdgeTriplet, Graph, VertexId}

/**
  * 一个图G的 KCore 是G的子图 这个子图的每个顶点的度≥K
  * 本算法为带权重的KCore算法，计算输入图中每个顶点的最大度数
  */
object KCore {
  /**
    * 因为权重是double，顶点初始的度数也只能是double，而度数k是整数，不能直接比较相等
    */
  def _equalTo(x: Double, k: Int) = math.abs(x - k) <= 1e-6

  def _greaterThan(x: Double, k: Int) = x > k + 1e-6

  def _equalOrLessThan(x: Double, k: Int) = x <= k + 1e-6

  /**
    * @param graph    输入图，边的属性值为权重
    * @param maxSteps 最大迭代次数，若要获取3core子图，可设maxSteps为3；
    * @return graph 每个顶点的最大度数（不超过maxSteps）；
    *         若要获取kcore子图，可用subgraph，如 3core子图：g.subgraph(x => true, (vid, vd) => vd >= 3)
    */
  def run(graph: Graph[_, Double], maxSteps: Int = Int.MaxValue): Graph[Int, Double] = {

    //获取初始度数，即顶点的所有边的权重之和；所有顶点的初始状态isFinal=false
    var kcGraph = graph.messageProgram[(Double, Boolean), Double](
      et => Iterator(et.srcId -> et.attr, et.dstId -> et.attr), _ + _)(
      (vid, vd, msg) => {
        val d = msg.getOrElse(0.0)
        if (_equalOrLessThan(d,1)) (1.0 -> false) //初始值小于等于1，属于1Core
        else (d -> false)
      }
    )


    def sendMsg(k: Int)(et: EdgeTriplet[(Double, Boolean), Double]): Iterator[(VertexId, Double)] = {
      //2端都非final，且一端等于k，另一端大于k，则另一端的度数需减去这条边的权重值
      if (!et.srcAttr._2 && !et.dstAttr._2 &&
        _equalTo(et.srcAttr._1, k) && _greaterThan(et.dstAttr._1, k)) {
        Iterator(et.dstId -> et.attr)
      } else if (!et.srcAttr._2 && !et.dstAttr._2 &&
        _equalTo(et.dstAttr._1, k) && _greaterThan(et.srcAttr._1, k)) {
        Iterator(et.srcId -> et.attr)
      } else {
        Iterator.empty
      }
    }

    //从1core开始迭代
    var k = 1
    var num = kcGraph.vertices.filter(x => _greaterThan(x._2._1, k)).length
    while (num > 0 && k <= maxSteps) {
      // kcore = k 且 非final的点需要发送消息更新对端；
      var actives = kcGraph.vertices.filter(x => _equalTo(x._2._1, k) && !x._2._2).length
      while (actives > 0) {
        kcGraph = kcGraph.messageProgram[(Double, Boolean), Double](sendMsg(k), _ + _) {
          case (vid, (kcore, isFinal), msg) =>
            msg match {
              case Some(m) =>
                //kcore - m 小于等于 k 说明此顶点属于k core, isFinal设置为false，下一轮需要更新对端点
                if (_equalOrLessThan(kcore - m, k)) k.toDouble -> false
                else kcore - m -> false
              case None =>
                // isFinal==true的点属性不变，kcore == k的点isFinal设置为true，下一轮不再发送消息出去
                if (!isFinal && _equalTo(kcore, k)) k.toDouble -> true
                else kcore -> isFinal
            }
        }
        //新图有顶点的度刚刚变为k，则需继续更新图
        actives = kcGraph.vertices.filter(x => _equalTo(x._2._1, k) && !x._2._2).length
      }

      //还存在度大于k的点，则需继续迭代
      num = kcGraph.vertices.filter(x => _greaterThan(x._2._1, k)).length
      k += 1
    }
    kcGraph.mapVertices((vid, vd) => vd._1.toInt)
  }

}
