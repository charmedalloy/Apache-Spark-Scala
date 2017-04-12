package aval.spark.q4

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.sql.Date
import org.apache.spark.sql.catalyst.expressions.Descending

object maxAge {

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
  val Date = new Date(System.currentTimeMillis)
  def age(dob: String) = {
    val date = dob.split("/")
    val Year = Date.getYear() + 1900

    var age = Year - date(2).toInt

    age.toInt
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length != 2) {
      println("usage: Question4      FriendsList_file_path      UserData_file_path")
      sys.exit(-1)
    }
    val friendsFile = args(0)
    val userData = args(1)

    val sc = new SparkContext("local[*]", "friends")
    val lines1 = sc.textFile(friendsFile)

    // Emit (2,1) (3,1) (4,1)
    val rdd1 = lines1.map(line => line.split("\t")).filter(fields => fields.length == 2)
      .map(str => (str(0), str(1).split(',').toList))
      .flatMap(f => f._2.map(y => (y, f._1)))

    //Emit (1, age) (2, age)
    val lines2 = sc.textFile(userData)
    val rdd2 = lines2.map(line => line.split(",")).map(fields => (fields(0), age(fields(9))))

    // (2,1,age)  (3,1,age) (4,1,age)...
    val joinRdd1 = rdd1.join(rdd2).map { case (a, (b, c)) => (a, b, c) }

    // (1,age) (1,age) (1,age) (2,age) (2,age)...... 
    val finalRdd = joinRdd1.map { case (a, b, c) => (b, c) }

    // (1,age) (2,age) (3,age)...
    val redRdd = finalRdd.reduceByKey((x, y) => (math.max(x, y)))

    val results1 = redRdd

    // details of address of each user id
    val userRdd = lines2.map(line => line.split(",")).map(fields => (fields(0), (fields(3), fields(4), fields(5))))

    val results = results1.join(userRdd).sortBy(_._2._1, false)
      .map { case (a, (b, (c, d, e))) => (a, (c, d, e), b) }
    results.take(10).foreach(x=> println(x.toString().replace("(", " ").replace(")", " ")))

  }
}