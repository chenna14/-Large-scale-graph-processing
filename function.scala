import scala.io.StdIn.readLine
import org.apache.spark.graphx._
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.nio.file.{Files, Paths}
import scala.collection.mutable.Queue
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean()
val starttime = System.nanoTime()
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
val graphPath = "/home/ragala/spark/code/facebook_combined.txt"
val disksUtilized = Files.getFileStore(Paths.get(graphPath)).getTotalSpace()

val graph = GraphLoader.edgeListFile(sc, "/home/ragala/spark/code/facebook_combined.txt")
val endtime = System.nanoTime()
val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
val elapsed = (endtime-starttime)/1e9
val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
println(s"The graph file is utilizing $disksUtilized disk(s)")
println(s"ElapsedTime for loading graph: $elapsed")
println(s"CPU utilization: $cpuUtilization%")
val alpha = 0.85
val maxIterations = 20
var ranks = graph.vertices.mapValues(v => 1.0)


object FunctionSwitch {

  def main(args: Array[String]): Unit = {
    var continue = true
    while (continue) {
      println("Enter a number to select a function, or 'q' to quit:")
      println("1. number_of_nodes")
      println("2. number_of_edges")
      println("3. Degree of a Node")
      println("4. Neighbors of a node ")
      println("5. To check a edge between two nodes ")
      println("6. Number of triangles : ")
      println("7. Average degree  ")
      println("8. Connected_components ")
      println("9. Pagerank :  ")
      println("10.Max_Degree :  ")
      println("11.Distance_between_nodes :  ")
      val input = readLine("Enter the input: ")
      println(s"$input")
      input match {
        case "1" => vertices()
        case "2" => edges()
        case "3" => degree()
        case "4" => neighbors()
        case "5" => edge_exist()
        case "6" => triangles()
        case "7" => average_degree()
        case "8" => connected_components()
        case "9" => pagerank()
        case "10" => maxdegree()
        case "11" => distbtwnodes()
        case "q" => continue = false
        case _ => println("Invalid input. Please try again.")
      }
    }
    val runtime = Runtime.getRuntime()
    val memoryUsed = runtime.totalMemory() - runtime.freeMemory()
    println(s"Memory Used : $memoryUsed bytes")
  }

 def vertices(): Unit = {
  val starttime = System.nanoTime()
  val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
    println("No of nodes are:")
    val a = graph.numVertices
    println(s"$a")
    val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
    val elapsed = (endtime-starttime)/1e9
    val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
    println(s"ElapsedTime for the query: $elapsed")
    println(s"CPU utilization: $cpuUtilization%")
  }

def edges(): Unit = {
   val starttime = System.nanoTime()
   val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
    println("No of edges :")
    val a = graph.numEdges
    println(s"$a")
     val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }

def degree(): Unit = {
    val a = readLine("Enter the node id :").toInt
    println(s"$a")
    val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
     val starttime = System.nanoTime()
    val b = graph.edges.filter(e => e.srcId == a || e.dstId == a).count().toInt
    println("Number of neighbors of vertex " + a + ":" + b)
     val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }

def neighbors(): Unit = {
    val vertexId = readLine("Enter id of the node :").toInt
    println(s"$vertexId")
    val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
     val starttime = System.nanoTime()
	val neighbors = graph.edges.filter(e => e.srcId == vertexId).map(e => e.dstId)
	val neigh = graph.edges.filter(e => e.dstId == vertexId).map(e => e.srcId)
	// Print the neighbors
	println("Neighbors of vertex " + vertexId + ":")
	neighbors.collect().foreach(println)
	neigh.collect().foreach(println)
	 val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }
def average_degree() : Unit = {
    println(s"CPU utilization")
    val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
     val starttime = System.nanoTime()
	val degrees = graph.degrees
	val avgDegree = degrees.map(_._2).mean()
	println(s"Average degree of the graph: $avgDegree")
    	 val endtime = System.nanoTime()
        val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  
  }
  def maxdegree() : Unit = {
      val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
     val starttime = System.nanoTime()
     val degrees = graph.degrees
    val maxDegree = degrees.map(_._2).max()
     println(s"Max Degree: $maxDegree")
      val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }
def triangles() : Unit = {
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
   val starttime = System.nanoTime()
     val spark = SparkSession.builder.appName("TriangleCounting").getOrCreate()

    // Load the graph from a file
    val graph = GraphLoader.edgeListFile(spark.sparkContext, "/home/ragala/spark/code/facebook_combined.txt")

    // Count the number of triangles in the graph
    val triangleCount = graph.triangleCount().vertices.map(_._2).reduce(_ + _)

    // Print the result
    println(s"Number of triangles in the graph: $triangleCount")

    spark.stop()
     val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  
  }
  
def connected_components(): Unit = {
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
 val starttime = System.nanoTime()
	    println("No of Connected_Components :")
	    val cc = graph.connectedComponents().vertices
	    println(s"$cc")
	     val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
 }
  
def edge_exist(): Unit = {

	  val srcVertexId = readLine("Enter the first node id :").toInt
	    println(s"$srcVertexId")
	  val dstVertexId= readLine("Enter the second node id :").toInt
	    println(s"$dstVertexId") 
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
          val starttime = System.nanoTime()
	// Check if the edge exists in the graph
	val edgeExists = graph.triplets.filter(triplet =>
	  triplet.srcId == srcVertexId && triplet.dstId == dstVertexId
	).count() > 0

	// Print the result
	if (edgeExists) {
	  println(s"Edge ($srcVertexId -> $dstVertexId) exists in the graph")
	} else {
	  println(s"Edge ($srcVertexId -> $dstVertexId) does not exist in the graph")
	}
	 val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  
  }
  
def pagerank() : Unit = {
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
   val starttime = System.nanoTime()
 //  val numVertices = graph.numVertices
//  val numEdges = graph.numEdges

 // for (i <- 1 to maxIterations) {
 //   val contributions = graph.aggregateMessages[Double](
 //     ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr / ctx.srcDegree),
 //     (a, b) => a + b
  //  )

 //   ranks = contributions
 //     .mapValues(v => alpha * v + (1 - alpha) / numVertices)
 //     .join(graph.vertices)
  //    .mapValues { case (rank, attr) => rank * attr }

  //  val danglingMass = ranks.filter(!_._2.isNaN).map { case (v, r) => if (graph.inDegree(v) == 0L) r else 0.0 }.sum
  //  ranks = ranks.mapValues(_ + (1 - alpha) * danglingMass / numVertices)
 // }

//  ranks
val targetId= readLine("Enter the first node id :").toInt
println(s"$targetId")
val pageRank = graph.pageRank(0.0001).vertices

// Filter the PageRank RDD to get the score for the target vertex
val targetPageRank = pageRank.filter { case (vertexId, score) => vertexId == targetId }.first()._2
// Print the PageRank score for the target vertex
println(s"Vertex $targetId has PageRank $targetPageRank")

//val pageRank = pageRank(graph, ranks, alpha, maxIterations)
//pageRank.foreach { case (vertexId, score) => println(s"Vertex $vertexId has PageRank $score") }
  //println(s"page rank : $ranks")
   val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }
  
def distbtwnodes() : Unit = {

	  val srcVertexId = readLine("Enter the first node id :").toInt
	    println(s"$srcVertexId")
	  val dstVertexId= readLine("Enter the second node id :").toInt
	    println(s"$dstVertexId")
          val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
          val starttime = System.nanoTime()
          val srcVertex: VertexId = srcVertexId
val dstVertex: VertexId = dstVertexId

// initialize all vertices with distance infinity except for the source vertex
val initialGraph = graph.mapVertices((id, _) =>
  if (id == srcVertex) 0.0 else Double.PositiveInfinity)

// define the message to be sent between vertices
val bfs = (id: VertexId, dist: Double, newDist: Double) => math.min(dist, newDist)

// start BFS from the source vertex
val bfsGraph = initialGraph.pregel(Double.PositiveInfinity)(
  (id, dist, newDist) => bfs(id, dist, newDist), // vertex program
  triplet => {  // send message only if the edge weight is 1
    if (triplet.srcAttr + 1 < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + 1))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // merge message
)

// get the shortest path from source to destination
val shortestPath = bfsGraph.vertices.filter(v => v._1 == dstVertex).first()._2.toInt

// print the shortest path length
println(s"The shortest path length between $srcVertex and $dstVertex is $shortestPath")

          
           val endtime = System.nanoTime()
        val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for the query: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
}
  
  
}

FunctionSwitch.main(Array())
