import scala.io.StdIn.readLine
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import java.lang.management.ManagementFactory
import java.lang.management.ThreadMXBean
import java.nio.file.{Files, Paths}

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
      println("7. Path between two nodes ")
      println("8. Connected_components ")
      println("9. Pagerank :  ")
      val input = readLine("Enter the input: ")
      println(s"$input")
      input match {
        case "1" => vertices()
        case "2" => edges()
        case "3" => degree()
        case "4" => neighbors()
        case "5" => edge_exist()
        case "6" => triangles()
        case "7" => path()
        case "8" => connected_components()
        case "9" => pagerank()
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
    println(s"ElapsedTime for loading graph: $elapsed")
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
	println(s"ElapsedTime for loading graph: $elapsed")
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
	println(s"ElapsedTime for loading graph: $elapsed")
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
	println(s"ElapsedTime for loading graph: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }
def path() : Unit = {
  val a = readLine("Enter the first node id :").toInt
    println(s"$a")
  val b = readLine("Enter the second node id :").toInt
    println(s"$b")
    
  
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
	println(s"ElapsedTime for loading graph: $elapsed")
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
	println(s"ElapsedTime for loading graph: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
 }
  
def edge_exist(): Unit = {
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
          val starttime = System.nanoTime()
	  val srcVertexId = readLine("Enter the first node id :").toInt
	    println(s"$srcVertexId")
	  val dstVertexId= readLine("Enter the second node id :").toInt
	    println(s"$dstVertexId") 

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
	println(s"ElapsedTime for loading graph: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  
  }
  
def pagerank() : Unit = {
val startCpuTime: Long = threadBean.getCurrentThreadCpuTime()
   val starttime = System.nanoTime()
  val ranks = graph.pageRank(0.0001).vertices
  //println(s"$ranks")
	//Take a look at the output
  ranks.collect()
  //println(s"page rank : $ranks")
   val endtime = System.nanoTime()
    val elapsedCpuTime: Long = threadBean.getCurrentThreadCpuTime() - startCpuTime
	val elapsed = (endtime-starttime)/1e9
	val cpuUtilization: Double = elapsedCpuTime.toDouble/1e9 / elapsed * 100
	println(s"ElapsedTime for loading graph: $elapsed")
	println(s"CPU utilization: $cpuUtilization%")
  }
  
  
}

FunctionSwitch.main(Array())

