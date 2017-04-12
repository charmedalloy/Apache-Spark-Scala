package aval.spark.q3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

object FriendsData {

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
  def loadData(userData: String): Map[Long, String] = {
    var DataMap: Map[Long, String] = Map()

    val lines = Source.fromFile(userData).getLines()
    for (line <- lines) {
      var fields = line.split(',')
      DataMap += (fields(0).toLong -> (fields(1) + "," + fields(9)))

    }
    DataMap
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    //Logger.getLogger("org").setLevel(Level.ERROR)
    val friendsFile = args(0)
    val userData = args(1)
    val sc = new SparkContext("local[*]", "friends")
    val lines = sc.textFile("./soc-LiveJournal1Adj.txt")

    var friendsData = sc.broadcast(loadData(userData))
    val uid1 = scala.io.StdIn.readLine("UserID1")
    val uid2 = scala.io.StdIn.readLine("UserID2")
    val rdd = lines.map(parse).filter(line => (line._1 != "-1" && (line._1 == uid1 || line._1 == uid2)))
    val t = rdd.flatMap(x => x._2._2.map(y => if (x._1.toLong < y.toLong) { ((x._1, y), x._2) } else { ((y, x._1), x._2) }))
    //val s = t.reduceByKey((x, y) => x.intersect(y))
    val p = t.reduceByKey((x, y) => ((x._1 + y._1), x._2.intersect(y._2))).filter(v => v._2._1 != 1)
    val results = p.map(r => (r._1, r._2._2)) // mutual friends list 1,2-> list of friends

    val finalR = results.map(r => (r._1, r._2.map(f => (friendsData.value(f.toLong)))))

    results.saveAsTextFile(args(2))
    finalR.foreach { println }
  }
}