package com.mingxw.lgraph.impl

import com.mingxw.lgraph._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag

class GraphImpl[VD: ClassTag, ED: ClassTag] protected(val vertices: Array[(VertexId, VD)],
                                                      val edges: Array[Edge[ED]])
  extends Graph[VD, ED] with Serializable {
  private val global2Local = vertices.map(_._1).zipWithIndex.toMap

  /**
    * An RDD containing the edge triplets, which are edges along with the vertex data associated with
    * the adjacent vertices. The caller should use [[edges]] if the vertex data are not needed, i.e.
    * if only the edge data and adjacent vertex ids are needed.
    *
    * @return an RDD containing edge triplets
    * @example This operation might be used to evaluate a graph
    *          coloring where we would like to check that both vertices are a
    *          different color.
    *          {{{
    *                                                                                                                                             type Color = Int
    *                                                                                                                                             val graph: Graph[Color, Int] = GraphLoader.edgeListFile("hdfs://file.tsv")
    *                                                                                                                                             val numInvalid = graph.triplets.map(e => if (e.src.data == e.dst.data) 1 else 0).sum
    *          }}}
    */
  override def triplets: Iterator[EdgeTriplet[VD, ED]] = Iterator.range(0, edges.length).map(i => {
    val et = new EdgeTriplet[VD, ED]()
    et.srcId = edges(i).srcId
    et.dstId = edges(i).dstId
    et.attr = edges(i).attr
    et.srcAttr = vertices(global2Local.get(et.srcId).get)._2
    et.dstAttr = vertices(global2Local.get(et.dstId).get)._2
    et
  })

  /**
    * Transforms each vertex attribute in the graph using the map function.
    *
    * @note The new graph has the same structure.  As a consequence the underlying index structures
    *       can be reused.
    * @param map the function from a vertex object to a new vertex value
    * @tparam VD2 the new vertex data type
    * @example We might use this operation to change the vertex values
    *          from one type to another to initialize an algorithm.
    *          {{{
    *                                                                                                                                             val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
    *                                                                                                                                             val root = 42
    *                                                                                                                                             var bfsGraph = rawGraph.mapVertices[Int]((vid, data) => if (vid == root) 0 else Math.MaxValue)
    *          }}}
    *
    */
  override def mapVertices[VD2: ClassManifest](map: (VertexId, VD) => VD2)(implicit eq: =:=[VD, VD2]): Graph[VD2, ED] = {
    new GraphImpl[VD2, ED](vertices.map(v => (v._1, map(v._1, v._2))), edges)
  }

  /**
    * Transforms each edge attribute in the graph using the map function.  The map function is not
    * passed the vertex value for the vertices adjacent to the edge.  If vertex values are desired,
    * use `mapTriplets`.
    *
    * @note This graph is not changed and that the new graph has the
    *       same structure.  As a consequence the underlying index structures
    *       can be reused.
    * @param map the function from an edge object to a new edge value.
    * @tparam ED2 the new edge data type
    * @example This function might be used to initialize edge
    *          attributes.
    *
    */
  override def mapEdges[ED2: ClassManifest](map: (Edge[ED]) => ED2): Graph[VD, ED2] = {
    new GraphImpl[VD, ED2](vertices, edges.map(e => new Edge[ED2](e.srcId, e.dstId, map(e))))
  }

  /**
    * Transforms each edge attribute using the map function, passing it the adjacent vertex
    * attributes as well. If adjacent vertex values are not required,
    * consider using `mapEdges` instead.
    *
    * @note This does not change the structure of the
    *       graph or modify the values of this graph.  As a consequence
    *       the underlying index structures can be reused.
    * @param map the function from an edge object to a new edge value.
    * @tparam ED2 the new edge data type
    * @example This function might be used to initialize edge
    *          attributes based on the attributes associated with each vertex.
    *          {{{
    *                                                                                                                                   val rawGraph: Graph[Int, Int] = someLoadFunction()
    *                                                                                                                                   val graph = rawGraph.mapTriplets[Int]( edge =>
    *                                                                                                                                     edge.src.data - edge.dst.data)
    *          }}}
    *
    */
  override def mapTriplets[ED2: ClassTag](map: (EdgeTriplet[VD, ED]) => ED2): Graph[VD, ED2] = {
    val newEdges = triplets.map(et => new Edge[ED2](et.srcId, et.dstId, map(et))).toArray
    new GraphImpl[VD, ED2](vertices, newEdges)
  }

  /**
    * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
    * graph contains an edge from b to a.
    */
  override def reverse: Graph[VD, ED] = {
    val newEdges = edges.map(e => new Edge(e.dstId, e.srcId, e.attr))
    new GraphImpl[VD, ED](vertices, newEdges)
  }

  /**
    * Restricts the graph to only the vertices and edges satisfying the predicates. The resulting
    * subgraph satisfies
    *
    * {{{
    * V' = {v : for all v in V where vpred(v)}
    * E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}
    * }}}
    *
    * @param epred the edge predicate, which takes a triplet and
    *              evaluates to true if the edge is to remain in the subgraph.  Note
    *              that only edges where both vertices satisfy the vertex
    *              predicate are considered.
    * @param vpred the vertex predicate, which takes a vertex object and
    *              evaluates to true if the vertex is to be included in the subgraph
    * @return the subgraph containing only the vertices and edges that
    *         satisfy the predicates
    */
  override def subgraph(epred: (EdgeTriplet[VD, ED]) => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    val newVertices = vertices.filter(v => vpred(v._1, v._2))
    val newEdges = triplets.filter(et => vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et))
      .map(et => new Edge[ED](et.srcId, et.dstId, et.attr)).toArray
    new GraphImpl[VD, ED](newVertices, newEdges)
  }


  /**
    * Merges multiple edges between two vertices into a single edge.
    *
    * @param merge the user-supplied commutative associative function to merge edge attributes
    *              for duplicate edges.
    * @return The resulting graph with a single edge for each (source, dest) vertex pair.
    */
  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    if (edges.isEmpty) return this
    val newEdges = new ArrayBuffer[Edge[ED]]()
    newEdges += edges(0)
    var srcId = edges(0).srcId
    var dstId = edges(0).dstId
    for (i <- 1 until edges.length) {
      if (srcId == edges(i).srcId && dstId == edges(i).dstId) {
        val len = newEdges.length
        newEdges(len - 1) = new Edge[ED](srcId, dstId, merge(newEdges(len - 1).attr, edges(i).attr))
      } else {
        newEdges += edges(i)
        srcId = edges(i).srcId
        dstId = edges(i).dstId
      }
    }
    new GraphImpl[VD, ED](vertices, newEdges.toArray)
  }

  /**
    * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
    * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
    * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
    * destined to the same vertex.
    *
    * This variant can take an active set to restrict the computation and is intended for internal
    * use only.
    *
    * @tparam A the type of message to be sent to each vertex
    * @param sendMsg  runs on each edge, sending messages to neighboring vertices using the
    *                 [[EdgeTriplet]].
    * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
    *                 combiner should be commutative and associative.
    */
  def aggregateMessages[A: ClassTag](sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                     mergeMsg: (A, A) => A,
                                     tripletFields: TripletFields = TripletFields.All)
  : Array[(VertexId, A)] = {
    val msgs = new HashMap[VertexId, A]()
    triplets.flatMap(sendMsg).foreach { case (vid, a) =>
      msgs.get(vid) match {
        case Some(m) => msgs += vid -> mergeMsg(a, m)
        case None => msgs += vid -> a
      }
    }
    msgs.toArray
  }

  /**
    * Execute a message program in vertex level(Think Like a Vertex)
    *
    * @param sendMsg  runs on each edge, sending messages to neighboring vertices using the [[EdgeTriplet]].
    * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
    *                 combiner should be commutative and associative.
    * @param vprog    the user-defined vertex program which runs on each vertex and
    *                 receives the inbound message and computes a new vertex value.
    * @tparam VD2 the type of new vertex value
    * @tparam A   the type of message to be sent to each vertex
    * @return the resulting graph at the end of the computation
    * @example We can use this function to compute the out-degree of each vertex and used as the vertex value
    *          {{{
    *                                                                val rawGraph: Graph[_, _] = someLoadFunction()
    *                                                                val graph = rawGraph.messageProgram[Int,Int](_.sendToDst(1),
    *                                                                     _ + _, TripletFields.None)((vid, vd, deg) => deg.getOrElse(0))
    *          }}}
    */
  def messageProgram[VD2: ClassTag, A: ClassTag](sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                 mergeMsg: (A, A) => A)
                                                (vprog: (VertexId, VD, Option[A]) => VD2)
  : Graph[VD2, ED] = {
    val msgs = new HashMap[VertexId, A]()
    triplets.flatMap(sendMsg).foreach { case (vid, a) =>
      msgs.get(vid) match {
        case Some(m) => msgs += vid -> mergeMsg(a, m)
        case None => msgs += vid -> a
      }
    }
    val newVertices = vertices.map { case (vid, vd) => vid -> vprog(vid, vd, msgs.get(vid)) }
    new GraphImpl[VD2, ED](newVertices, edges)
  }

  /**
    * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.
    * The input table should contain at most one entry for each vertex.  If no entry in `other` is
    * provided for a particular vertex in the graph, the map function receives `None`.
    *
    * @tparam U   the type of entry in the table of updates
    * @tparam VD2 the new vertex value type
    * @param other   the table to join with the vertices in the graph.
    *                The table should contain at most one entry for each vertex.
    * @param mapFunc the function used to compute the new vertex values.
    *                The map function is invoked for all vertices, even those
    *                that do not have a corresponding entry in the table.
    * @example This function is used to update the vertices with new values based on external data.
    *          For example we could add the out-degree to each vertex record:
    *
    *          {{{
    *                                                                                                                         val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
    *                                                                                                                         val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees
    *                                                                                                                         val graph = rawGraph.outerJoinVertices(outDeg) {
    *                                                                                                                           (vid, data, optDeg) => optDeg.getOrElse(0)
    *                                                                                                                         }
    *          }}}
    */
  override def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: Array[(VertexId, U)])
                                                            (mapFunc: (VertexId, VD, Option[U]) => VD2)
                                                            (implicit eq: =:=[VD, VD2]): Graph[VD2, ED] = {
    val o = other.toMap
    val newVertices = vertices.map { case (vid, vd) => vid -> mapFunc(vid, vd, o.get(vid)) }
    new GraphImpl[VD2, ED](newVertices, edges)
  }


}

object GraphImpl {

  /**
    * Create a graph from edges, setting referenced vertices to `defaultVertexAttr`.
    */
  def apply[VD: ClassTag, ED: ClassTag](edges: Array[Edge[ED]],
                                        defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    val vertices = edges.flatMap(e => Iterator(e.srcId -> defaultVertexAttr, e.dstId -> defaultVertexAttr))
      .toMap.toArray.sortBy(_._1)
    val edgeSorted = edges.sortWith((e1, e2) => {
      if (e1.srcId < e2.srcId) true
      else if (e1.srcId > e2.srcId) false
      else if (e1.dstId < e2.dstId) true
      else if (e1.dstId > e2.dstId) false
      else true
    })
    new GraphImpl[VD, ED](vertices, edgeSorted)
  }

  /**
    * Create a graph from vertices and edges, setting missing vertices to `defaultVertexAttr`.
    */
  def apply[VD: ClassTag, ED: ClassTag](vertices: Array[(VertexId, VD)],
                                        edges: Array[Edge[ED]],
                                        defaultVertexAttr: VD = null.asInstanceOf[VD]): GraphImpl[VD, ED] = {
    val edgeSorted = edges.sortWith((e1, e2) => {
      if (e1.srcId < e2.srcId) true
      else if (e1.srcId > e2.srcId) false
      else if (e1.dstId < e2.dstId) true
      else if (e1.dstId > e2.dstId) false
      else true
    })
    val vertMap = new mutable.HashMap[VertexId, VD]()
    vertMap ++= vertices.toMap
    edgeSorted.foreach(e => {
      if (!vertMap.contains(e.srcId)) vertMap += e.srcId -> defaultVertexAttr
      if (!vertMap.contains(e.dstId)) vertMap += e.dstId -> defaultVertexAttr
    })
    new GraphImpl(vertMap.toArray.sortBy(_._1), edgeSorted)
  }


}
