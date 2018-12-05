import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithDouble._

import scala.io.Source
import java.io._

import org.apache.spark
object sparkTweets {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop\\")
    val sparkConf = new SparkConf().setAppName("spart_test").setMaster("local[*]")
    val sc = new SparkContext(sparkConf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    /* val textFile = sc.textFile("C:\\Users\\ruthv\\IdeaProjects\\sparkdemo\\src\\data\\extractedTweets.txt")
      val counts = textFile.flatMap(line => line.split(" "))
       .map(word => (word, 1))
       .reduceByKey(_ + _)
     counts.saveAsTextFile("src/data/output") */

    val textFile = sqlContext.read.json("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase1\\tweets.txt")
    textFile.createOrReplaceTempView("twit")

    //  Query 1 - Time zone based
    val timezone = sqlContext.sql("select  user.time_zone,count(*)  from twit where user.time_zone is not  null group by user.time_zone order by count(1) desc limit 10")
    var index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\timezone")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    timezone.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/timezone")

    // Query 2 - based on input languague
    val languague = sqlContext.sql("select user.lang,count(*)  from twit where user.lang is not null  group by user.lang order by count(1) desc limit 10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\languague")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    timezone.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/languague")

    // Query 3 - users created per year
    val usersCreated = sqlContext.sql("select substring(user.created_at,27,4) as year,count(*)  from twit where user.created_at is not null group by substring(user.created_at,27,4) order by count(1) desc")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\userCreated")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    timezone.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/userCreated")

    //hashtag separation
    val hashtagsarray = sqlContext.sql("select entities.hashtags from twit where entities.hashtags is not null")

    val hashtags = hashtagsarray.select(org.apache.spark.sql.functions.explode(hashtagsarray.col("hashtags")))
    val hashtagtext =hashtags.select("col.text")

    hashtagtext.createOrReplaceTempView("hashtags")

    // Query 4 - Top hashtags
    val topHashtags = sqlContext.sql("select text,count(*) from hashtags group by text order by count(1) desc limit 10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\topHashtags")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    timezone.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/topHashtags")


    // Query 5 - IPL related
    val ipl = sqlContext.sql("select text,count(*) from hashtags where text like '%ipl' or text like '%rcb%' or text like '%csk' group by text order by count(1) desc limit 10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\ipl")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    timezone.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/ipl")

    // Query 6 - Top followed Users
    val topfollowed= sqlContext.sql("select user.name,user.followers_count  from twit order by user.followers_count desc LIMIT 10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\topFollowed")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    topfollowed.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/topfollowed")

    // Query 7 - User Who tweeted more number of times.
    val freqTweetUsers = sqlContext.sql("select user.id,user.name,count(*) from twit group by user.id,user.name order by count(1) desc limit 10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\freqTweetUsers")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    freqTweetUsers.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/freqTweetUsers")
    freqTweetUsers.createOrReplaceTempView("moretweetusers")

    // Query 8 - Status count
    val statusCount = sqlContext.sql("select user.name,max(user.statuses_count) from twit where user.id in (select id from moretweetusers) group by user.name")

    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\statusCount")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    statusCount.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/statusCount")

    // Query 9 - Verified vs unverified users
    val uniqueUsers=sqlContext.sql("select distinct user.id,user.verified from twit")
    uniqueUsers.createOrReplaceTempView("uniq")
    val verifiedVsUnverified = sqlContext.sql("select verified,count(*) from uniq group by verified")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\verifiedVsUnverified")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    verifiedVsUnverified.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/verifiedVsUnverified")

    // Query 10 - Date and days
    val daysAndDate=sqlContext.sql("SELECT SUBSTR(created_at, 0, 10) tweet_date, COUNT(1) tweet_count FROM twit GROUP  BY SUBSTR(created_at, 0, 10) ORDER  BY COUNT(1) DESC LIMIT  10")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\daysAndDate")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    verifiedVsUnverified.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/daysAndDate")

    // Query 11,12,13 -  other statistics

    val totalTweets = sqlContext.sql("SELECT count(*) as total_tweets FROM twit")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\totalTweets")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    totalTweets.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/totalTweets")

    val totalUsers = sqlContext.sql("SELECT count(DISTINCT user.id) as total_users FROM twit")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\totalUsers")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    totalUsers.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/totalUsers")

    val totalRetweets = sqlContext.sql("SELECT count(retweeted_status.id) as total_retweets FROM twit")
    index = new File("C:\\Users\\ruthv\\Desktop\\PB\\Project\\PB_Phase2\\IntelliJ_Test\\totalRetweets")
    if(index.exists())
    {
      val entries = index.list()
      for(s <- entries)
      {
        var currentFile = new File(index.getPath(),s)
        currentFile.delete()
      }
      index.delete()
    }
    totalRetweets.coalesce(1).write.json("C:/Users/ruthv/Desktop/PB/Project/PB_Phase2/IntelliJ_Test/totalRetweets")

  }

}
