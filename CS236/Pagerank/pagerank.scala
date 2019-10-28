import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

import scala.util.control.Breaks._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.util.GraphGenerators

object helloworld {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("helloworld"))
    // A graph with edge attributes containing distances
    var graph = GraphLoader.edgeListFile(sc, "/home/hadoop/Documents/Wiki-Vote.txt").mapVertices((id, _)=>1.0)
//    graph.inDegrees.reduce((a,b)=> if (a._2 > b._2) a else b)

    var q = 0.85
    var page_number = 7115
    var vertices = graph.vertices
    var threshold = 0.001
    var max_cycle = 50

    var outDegree = graph.aggregateMessages[Int](_.sendToSrc(1), _+_)
    var rank = graph.vertices.mapValues( v => 1.00)
    var outable_rank = rank
    breakable{
      for (i <- 0 until max_cycle) {
        var rank_previous = rank
        var rank_vertices = outDegree.join(rank)
        var rank_edges = graph.edges
        var rank_graph = Graph(rank_vertices, rank_edges)

        rank_vertices = rank_graph.vertices.mapValues(x=>{
          if(x==null){
            (0,1.0)
          }else{
            x
          }
        })
        rank_graph = Graph(rank_vertices, rank_edges)

        rank = rank_graph.aggregateMessages[Double](
          triplet => {
            triplet.sendToDst(triplet.srcAttr._2*(1/triplet.srcAttr._1))
          },
          _+_
        )
        rank = rank.mapValues((1-q)/page_number+q*_)
        outable_rank = rank
        var none_indegree = vertices.mapValues(v=>1.00).minus(rank)
        var tmp_rank: VertexRDD[Double] = VertexRDD(none_indegree.union(rank))
        rank = tmp_rank

        var varice = rank_previous.join(rank).mapValues((x$2)=>{
          if(x$2._1-x$2._2<0){
            x$2._2-x$2._1
          }
          else{
            x$2._1-x$2._2
          }
        })
        if (varice.filter(_._2>threshold).count==0){
          println("terminate in "+i+" cycle.\n")
          break;
        }
      }
    }

    val result=outable_rank.sortBy(_._2, false)
    println(result.take(20).mkString("\n"))
  }
}

