import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.util.GraphGenerators

object helloworld {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("helloworld"))
    // A graph with edge attributes containing distances
    val graph = GraphLoader.edgeListFile(sc, "/home/hadoop/Documents/Wiki-Vote.txt")
//    graph.inDegrees.reduce((a,b)=> if (a._2 > b._2) a else b)
    val v = graph.pageRank(0.001).vertices.sortBy(_._2, false)
    println(v.take(20).mkString("\n"))
  }
}
