import org.apache.spark._
import org.apache.log4j.{Level, Logger}
object commonfriends{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("commonfriends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    def friendsMapper(line: String) = {
      val words = line.split(" ")
      val k = words(0)
      val pairs = words.slice(1, words.size).map(f => {
        if (k < f) (k, f) else (f, k)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }

    def friendsReducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }

    val file = sc.textFile("src/main/resources/fbfriends1")

    val results = file.flatMap(friendsMapper)
      .reduceByKey(friendsReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")})

    results.coalesce(1).saveAsTextFile("CommonFriends")
  }

}