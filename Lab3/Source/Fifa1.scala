import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Fifa1 {

  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("FIFA World Cup Analysis")
      .config(conf = conf)
      .getOrCreate()


    // Creating WorldCups Dataframe using StructType
    val customSchema = StructType(Array(
      StructField("Year", IntegerType, true),
      StructField("Country", StringType, true),
      StructField("WinnerTeam", StringType, true),
      StructField("Runners-UpTeam", StringType, true),
      StructField("ThirdTeam", StringType, true),
      StructField("FourthTeam", StringType, true),
      StructField("GoalsScoredTotal", IntegerType, true),
      StructField("QualifiedTeamsTotal", IntegerType, true),
      StructField("MatchesPlayedTotal", IntegerType, true),
      StructField("AttendanceTotal", DoubleType, true)))

    val worldcup_df = spark.sqlContext.read.format("csv")
      .option("delimiter",",")
      .option("header", "true")
      .schema(customSchema)
      .load("src/main/resources/WorldCups.csv")

    // 1. Top teams that won highest number of WorldCups

    worldcup_df.groupBy("WinnerTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // 2. Countries that hosted most number of WorldCups

    worldcup_df.groupBy("Country").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // 3. Countries that hosted and won the WorldCup

    worldcup_df.createGlobalTempView("worldcup")

    spark.sql("SELECT Country,WinnerTeam FROM global_temp.worldcup where Country==WinnerTeam").show()

    // 4. Total number of Goals Scored from the years 1930 to 2014

    worldcup_df.agg(sum("GoalsScoredTotal")).show()

    // 5. Top teams that missed WorldCup and placed in Runner-Up

    worldcup_df.groupBy("Runners-UpTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    //6. Percentage of Goals required to Qualify for the match
    import spark.sqlContext.implicits._

    worldcup_df.withColumn("Percentage",$"QualifiedTeamsTotal"/$"GoalsScoredTotal"*100).show()

    //7. Highest attendance for the matches

    worldcup_df.agg(max("AttendanceTotal")).show

    // 8. Top teams that missed WorldCup and placed in ThirdTeam

    worldcup_df.groupBy("ThirdTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // 9. Top teams that missed WorldCup and placed in FourthTeam

    worldcup_df.groupBy("FourthTeam").count().orderBy(org.apache.spark.sql.functions.col("count").desc).show()

    // 10. Total number of Goals scored by each country in all the years

    spark.sql("SELECT Country,sum(GoalsScoredTotal) FROM global_temp.worldcup group by Country").show()


    //RDD Creation

    val worldcup_rdd = sc.textFile("src/main/resources/WorldCups.csv")

    val column_names = worldcup_rdd.first()

    val data = worldcup_rdd.filter(line => line != column_names)

    data foreach println

    data.map(line=>(line.split(",")(2),1)).reduceByKey(_+_) foreach println

    data.map(line=>(line.split(",")(1),1)).reduceByKey(_+_) foreach println
  }

}
