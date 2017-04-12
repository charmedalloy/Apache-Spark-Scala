package aval.spark.q1

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MutualFriends {

  def parse(line: String): (String, (Long, List[String])) = {
    val fields = line.split("\t")
    if (fields.length == 2) {
      val user = fields(0)
      val friendsList = fields(1).split(",").toList
      return (user, (1L, friendsList))
    } else {
      //
      return ("-1", (-1L, List()))
    }
  }

  def main(args: Array[String]) {
    
    if ( args.length != 2 ) {
      println("usage: Question1      FriendsList_file_path      Outputfilepath")
      sys.exit(-1)
    }
    
    val frndsFile=args(0) 

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Friends")
    
    val lines = sc.textFile(args(0)) // reading file
    
    val rdd = lines.map(parse).filter(line => line._1 != "-1") // comparing the first attribute
    
    
    // Emit (1,2),1,list of friends) (1,3),1,list of friends)  
    val rddPair = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) { ((x._1, y), x._2) } else { ((y, x._1), x._2) }))
    
    // reduce  RDD[((String, String), (Long, List[String]))] (1,2),(2,list)
    val redRdd = rddPair.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)

    val red_rddPair = redRdd.map(r => (r._1, r._2._2)).filter(l=>l._2!=List())
    
    val results=red_rddPair
    results.saveAsTextFile(args(1))
    results.take(10).foreach { println }
  }
}
