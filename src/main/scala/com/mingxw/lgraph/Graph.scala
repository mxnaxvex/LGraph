package com.mingxw.lgraph


import com.mingxw.lgraph.impl.GraphImpl

import scala.reflect.ClassTag

abstract class Graph[VD: ClassTag, ED: ClassTag] extends Serializable {

  /**
    * An array containing the vertices and their associated attributes.
    *
    * @note vertex ids are unique.
    * @return an array containing the vertices in this graph
    */
  val vertices: Array[(VertexId, VD)]
  /**
    * An RDD containing the edges and their associated attributes.  The entries in the RDD contain
    * just the source id and target id along with the edge data.
    *
    * @return an RDD containing the edges in this graph
    * @see `Edge` for the edge type.
    * @see `Graph#triplets` to get an RDD which contains all the edges
    *      along with their vertex data.
    *
    */
  val edges: Array[Edge[ED]]
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
    *                     type Color = Int
    *                     val graph: Graph[Color, Int] = GraphLoader.edgeListFile("hdfs://file.tsv")
    *                     val numInvalid = graph.triplets.map(e => if (e.src.data == e.dst.data) 1 else 0).sum
    *          }}}
    */
  def triplets: Iterator[EdgeTriplet[VD, ED]]

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
    *                     val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
    *                     val root = 42
    *                     var bfsGraph = rawGraph.mapVertices[Int]((vid, data) => if (vid == root) 0 else Math.MaxValue)
    *          }}}
    *
    */
  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
                                (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]

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
  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2]

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
    *           val rawGraph: Graph[Int, Int] = someLoadFunction()
    *           val graph = rawGraph.mapTriplets[Int]( edge =>
    *             edge.src.data - edge.dst.data)
    *          }}}
    *
    */
  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]

  /**
    * Reverses all edges in the graph.  If this graph contains an edge from a to b then the returned
    * graph contains an edge from b to a.
    */
  def reverse: Graph[VD, ED]

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
  def subgraph(epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
               vpred: (VertexId, VD) => Boolean = ((v, d) => true))
  : Graph[VD, ED]


  /**
    * Merges multiple edges between two vertices into a single edge.
    *
    * @param merge the user-supplied commutative associative function to merge edge attributes
    *              for duplicate edges.
    * @return The resulting graph with a single edge for each (source, dest) vertex pair.
    */
  def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
  /**
    * Aggregates values from the neighboring edges and vertices of each vertex. The user-supplied
    * `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be
    * sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
    * destined to the same vertex.
    *
    * @tparam A the type of message to be sent to each vertex
    *
    * @param sendMsg runs on each edge, sending messages to neighboring vertices using the
    *   [[EdgeTriplet]].
    * @param mergeMsg used to combine messages from `sendMsg` destined to the same vertex. This
    *   combiner should be commutative and associative.
    *
    * @example We can use this function to compute the in-degree of each
    * vertex
    * {{{
    * val rawGraph: Graph[_, _] = Graph.textFile("twittergraph")
    * val inDeg: RDD[(VertexId, Int)] =
    *   rawGraph.aggregateMessages[Int](ctx => ctx.sendToDst(1), _ + _)
    * }}}
    *
    * @note By expressing computation at the edge level we achieve
    * maximum parallelism.  This is one of the core functions in the
    * Graph API that enables neighborhood level computation. For
    * example this function can be used to count neighbors satisfying a
    * predicate or implement PageRank.
    *
    */
  def aggregateMessages[A: ClassTag](sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                     mergeMsg: (A, A) => A,
                                     tripletFields: TripletFields = TripletFields.All)
  : Array[(VertexId, A)]

  /**
    * Joins the vertices with entries in the `table` RDD and merges the results using `mapFunc`.
    * The input table should contain at most one entry for each vertex.  If no entry in `other` is
    * provided for a particular vertex in the graph, the map function receives `None`.
    *
    * @tparam U the type of entry in the table of updates
    * @tparam VD2 the new vertex value type
    *
    * @param other the table to join with the vertices in the graph.
    *              The table should contain at most one entry for each vertex.
    * @param mapFunc the function used to compute the new vertex values.
    *                The map function is invoked for all vertices, even those
    *                that do not have a corresponding entry in the table.
    *
    * @example This function is used to update the vertices with new values based on external data.
    *          For example we could add the out-degree to each vertex record:
    *
    * {{{
    * val rawGraph: Graph[_, _] = Graph.textFile("webgraph")
    * val outDeg: RDD[(VertexId, Int)] = rawGraph.outDegrees
    * val graph = rawGraph.outerJoinVertices(outDeg) {
    *   (vid, data, optDeg) => optDeg.getOrElse(0)
    * }
    * }}}
    */
  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: Array[(VertexId, U)])
                                                   (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
  : Graph[VD2, ED]
  /**
    * Execute a message program in vertex level(Think Like a Vertex)
    *
    * @param sendMsg       runs on each edge, sending messages to neighboring vertices using the [[EdgeTriplet]].
    * @param mergeMsg      used to combine messages from `sendMsg` destined to the same vertex. This
    *                      combiner should be commutative and associative.
    * @param vprog         the user-defined vertex program which runs on each vertex and
    *                      receives the inbound message and computes a new vertex value.
    * @tparam VD2 the type of new vertex value
    * @tparam A   the type of message to be sent to each vertex
    * @return the resulting graph at the end of the computation
    * @example We can use this function to compute the out-degree of each vertex and used as the vertex value
    *          {{{
    *                        val rawGraph: Graph[_, _] = someLoadFunction()
    *                        val graph = rawGraph.messageProgram[Int,Int](_.sendToDst(1),
    *                             _ + _, TripletFields.None)((vid, vd, deg) => deg.getOrElse(0))
    *          }}}
    */
  def messageProgram[VD2: ClassTag, A: ClassTag](sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                 mergeMsg: (A, A) => A)
                                                (vprog: (VertexId, VD, Option[A]) => VD2)
  : Graph[VD2, ED]


  /**
    * The associated [[GraphOps]] object.
    */
  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
}
object Graph {

  /**
    * Construct a graph from a collection of edges encoded as vertex id pairs.
    *
    * @param rawEdges a collection of edges in (src, dst) form
    * @param defaultValue the vertex attributes with which to create vertices referenced by the edges
    *
    *  @return a graph with edge attributes containing either the count of duplicate edges or 1
    * (if `uniqueEdges` is `None`) and vertex attributes containing the total degree of each vertex.
    */
  def fromEdgeTuples[VD: ClassTag](rawEdges: Array[(VertexId, VertexId)],
                                    defaultValue: VD): Graph[VD, Int] =
  {
    val edges = rawEdges.map(p => new Edge[Int](p._1, p._2, 1))
    fromEdges(edges, defaultValue)
  }

  /**
    * Construct a graph from a collection of edges.
    *
    * @param edges the RDD containing the set of edges in the graph
    * @param defaultValue the default vertex attribute to use for each vertex
    *
    * @return a graph with edge attributes described by `edges` and vertices
    *         given by all vertices in `edges` with value `defaultValue`
    */
  def fromEdges[VD: ClassTag, ED: ClassTag](edges: Array[Edge[ED]],
                                             defaultValue: VD): Graph[VD, ED] = {
    GraphImpl(edges, defaultValue)
  }

  /**
    * Construct a graph from a collection of vertices and
    * edges with attributes.  Duplicate vertices are picked arbitrarily and
    * vertices found in the edge collection but not in the input
    * vertices are assigned the default attribute.
    *
    * @tparam VD the vertex attribute type
    * @tparam ED the edge attribute type
    * @param vertices the "set" of vertices and their attributes
    * @param edges the collection of edges in the graph
    * @param defaultVertexAttr the default vertex attribute to use for vertices that are
    *                          mentioned in edges but not in vertices
    */
  def apply[VD: ClassTag, ED: ClassTag](vertices: Array[(VertexId, VD)],
                                         edges: Array[Edge[ED]],
                                         defaultVertexAttr: VD = null.asInstanceOf[VD]): Graph[VD, ED] = {
    GraphImpl(vertices, edges, defaultVertexAttr)
  }


  /**
    * Implicitly extracts the [[GraphOps]] member from a graph.
    *
    * To improve modularity the Graph type only contains a small set of basic operations.
    * All the convenience operations are defined in the [[GraphOps]] class which may be
    * shared across multiple graph implementations.
    */
  implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag]
  (g: Graph[VD, ED]): GraphOps[VD, ED] = g.ops
}
