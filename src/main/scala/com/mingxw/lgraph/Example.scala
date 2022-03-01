package com.mingxw.lgraph

import com.mingxw.lgraph.lib.{KCore, KTruss}

import scala.collection.mutable

object Example {
  def testGraph(): Unit ={
    val words = "担保 条件 生命 人寿 持有 大连 樱华 健康 科技 有限公司 股权 质押 质押率 超过 相关 文本 深圳 分行 法律室 审定 建议 关注 北京 四季 花坛 商贸 有限公司 子公司 大连 樱华健康 科技 有限公司 增资 引进 投资者 公告 公告 编号 有关 股权 质押 转让 规定 安排 减少 股权 质押 转让 行为 限制"
      .split(" ")
    val vertices = words.toSet[String].toArray.zipWithIndex.map(x => x._2.toLong -> x._1)
    val vertMap = vertices.map(x => x._2 -> x._1).toMap
    val edges = new mutable.HashMap[(Long, Long), Int]()
    words.indices.foreach(i => {
      val src = vertMap.get(words(i)).get
      for (j <- i + 1 until math.min(i + 5, words.length)) {
        val dst = vertMap.get(words(j)).get
        val (id1, id2) = if (src < dst) (src, dst) else (dst, src)
        edges.get(id1 -> id2) match {
          case Some(n) => edges += (id1 -> id2) -> (n + 1)
          case None => edges += (id1 -> id2) -> 1
        }
      }
    })
    val g = Graph(vertices, edges.toArray.map { case ((src, dst), n) =>
      new Edge[Double](src, dst, n.toDouble)
    })
    println(g.numEdges)
    val kc = KCore.run(g.mapEdges(e => 1.0), 2)
    println(kc.vertices.map(v => vertices(v._1.toInt)._2+"/"+v._2).mkString(" "))
    val kt = KTruss.run(g)
    println(kt.vertices.map(v => vertices(v._1.toInt)._2+"/"+v._2).mkString(" "))

  }
  def main(args: Array[String]): Unit = {
    testGraph()
  }

}
