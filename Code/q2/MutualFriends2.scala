package aval.spark.q2

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MutualFriends2 {

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

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val friendsFile = args(0)
    
    val sc = new SparkContext("local[*]", "friends")
    
    val lines = sc.textFile(friendsFile)
    
    val uid1 = scala.io.StdIn.readLine("UserID1")
    val uid2 = scala.io.StdIn.readLine("UserID2")
    
    val rdd = lines.map(parse).filter(line => (line._1 != "-1" && (line._1 == uid1 || line._1 == uid2)))
    val finalRdd = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) { ((x._1, y), x._2) } else { ((y, x._1), x._2) }))

    val redRdd = finalRdd.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)

    val results = redRdd.map(r => (r._1, r._2._2))

    results.saveAsTextFile(args(1))
    results.foreach { println }
  }
}
