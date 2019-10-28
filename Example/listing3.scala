import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._

object helloworld {
  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("helloworld"))
    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    //    add attributes to edge
    myGraph.mapTriplets(t => (t.attr, t.attr=="is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))).triplets.collect
    //    print all the edge or node
    myGraph.triplets.collect.foreach(a=>print(a+"\n"))
    //    add attributes to edge by "map"
    myGraph.mapTriplets((t => (t.attr, t.attr=="is-friends-with" && t.srcAttr.toLowerCase().contains("a"))) : (EdgeTriplet[String,String] => Tuple2[String,Boolean]) ).triplets.collect().foreach(a=>print(a+"\n"))
    // calculate the outer degree
    myGraph.aggregateMessages[Int](_.sendToDst(1), _ + _).collect.foreach(a=>print(a+"\n"))
    sc.stop()
  }
}

