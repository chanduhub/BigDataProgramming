import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val input = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")
    val output = spark.createDataFrame(List(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    )).toDF("src", "dst", "relationship")

    val g = GraphFrame(input,output)
    g.vertices.show()
    g.edges.show()

    val stationdf = spark.read.format("csv").option("header", "true").load("201508_station_data.csv")
    val tripdatadf = spark.read.format("csv").option("header", "true").load("201508_trip_data.csv")

    val vertices = stationdf.select("name").toDF("id")
    val edges = tripdatadf.select("Start Station","End Station").toDF("src","dst")

    //val g1 = GraphFrame(stationdf,tripdatadf)

    //g1.vertices.show()
    vertices.show()
    edges.show()



    val g1 = GraphFrame(vertices,edges)

    g1.vertices.show()

    g1.edges.show()

    g1.inDegrees.show()

    g1.outDegrees.show()

    g1.degrees.show()

    val motifs = g1.find("(a)-[e]->(b);(b)-[e2]->(a)")
    motifs.show()

    g1.vertices.write.parquet("file")

    print("******************"+vertices.count())
    print("******************************"+vertices.distinct().count())






  }
}
