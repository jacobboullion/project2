package com.revature.hellosparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrameReader
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import scala.concurrent.Future
import org.apache.http.util.EntityUtils

object Runner {
    def main(args: Array[String]): Unit = {

        //initialize a SparkSession, by convention called spark
        val spark = SparkSession.builder()
            .appName("Hello Spark SQL")
            .master("local[4]")
            .getOrCreate()


        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        //hellosparkSql(spark)

        //helloTweetStream(spark)

        helloFollowerStream(spark)

        //ParquetDemo.run(spark)

        //JoinDemo.run(spark)

    }
    
    def helloTweetStream(spark: SparkSession): Unit = {
        import spark.implicits._

        val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

        //writes all the tweets from twitter's stream into a directory
        //by default hits the sampled stream and uses "twitterstream" as the directory
        //run in the background using a Future:
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
            tweetStreamToDir(bearerToken, queryString = "?tweet.fields=geo&expansions=geo.place_id")
        }
        
        //busy  wait until a file appears in our twitter stream directory
        var start = System.currentTimeMillis()
        var filesFoundInDir = false
        while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
            filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
            Thread.sleep(500)
        }
        if(!filesFoundInDir){
            println("Error: unable to populate tweetstream after 30")
            System.exit(1)
        }

        val staticDf = spark.read.json("twitterstream")

        val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

        //display placenames
        // streamDf
        //   .filter(!functions.isnull($"includes.places"))
        //   .select(functions.element_at($"includes.places", 1)("full_name").as("Place"), ($"data.text").as("Tweet"))
        //   .writeStream
        //   .outputMode("append")
        //   .format("console")
        //   .option("truncate", false)
        //   .start()
        //   .awaitTermination()

        //Just getting the text
        // streamDf
        //   .select($"data.text")
        //   .writeStream
        //   .outputMode("append")
        //   .format("console")
        //   .start()
        //   .awaitTermination()

        //Most used twitter handles, aggregated over time:

        //regex to extract twitter handles
        val pattern = ".*(@\\w+)\\s+.*".r

        streamDf
          .select($"data.text")
          .as[String]
          .flatMap(text => {text match {
              case pattern(handle) => {Some(handle)}
              case notFound => None
          }})
          .groupBy("value")
          .count()
          .sort(functions.desc("count"))
          .writeStream
          .outputMode("complete")
          .format("console")
          .start()
          .awaitTermination()

    }

    def tweetStreamToDir(
        bearerToken: String, 
        dirname:String="twitterstream", 
        linesPerFile:Int=1000,
        queryString: String = ""
    ) = {

        val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
            )
            .build()

        val uriBuilder: URIBuilder = new URIBuilder(
            s"https://api.twitter.com/2/tweets/sample/stream$queryString"
            )
        val httpGet = new HttpGet(uriBuilder.build())

        httpGet.setHeader("Authorization", s"Bearer $bearerToken")
        val response = httpClient.execute(httpGet)
        val entity = response.getEntity()
        if (null != entity) {
            val reader = 
              new BufferedReader(new InputStreamReader(entity.getContent())
            )
            var line = reader.readLine()
            //file writer
            var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile())
            var lineNumber = 1 
            val millis = System.currentTimeMillis()
            while(line != null) {
                if (lineNumber % linesPerFile == 0){
                    fileWriter.close()
                    Files.move(
                        Paths.get("tweetstream.tmp"),
                        Paths.get(s"$dirname/tweetstream-$millis-${lineNumber/linesPerFile}"))
                    fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
                }

                fileWriter.println(line)
                line = reader.readLine()
                lineNumber += 1
            }
            
        }

    }

    def tweetStreamToDirUser(
        bearerToken: String, 
        dirname:String="followersWed", 
        linesPerFile:Int=2,
        queryString: String = ""
    ) = {

        val httpClient = HttpClients.custom.setDefaultRequestConfig(
            RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
            )
            .build()

        val uriBuilder: URIBuilder = new URIBuilder(
            s"https://api.twitter.com/2/users/by$queryString"
            )
        val httpGet = new HttpGet(uriBuilder.build())

        httpGet.setHeader("Authorization", s"Bearer $bearerToken")
        val response = httpClient.execute(httpGet)
        val entity = response.getEntity()
        if (null != entity) {
            
            val response2 = EntityUtils.toString(entity, "UTF-8")

            //file writer
            var fileWriter = new PrintWriter(Paths.get("follower.tmp").toFile())
            var lineNumber = 1 
            val millis = System.currentTimeMillis()
        
            fileWriter.write(response2)
            fileWriter.flush
            fileWriter.close()
            Files.move(
                Paths.get("follower.tmp"),
                Paths.get(s"$dirname/follower-$millis-${lineNumber/linesPerFile}"))
            
            
            //println(response2)
        }


    }

    def helloFollowerStream(spark: SparkSession): Unit = {
        import spark.implicits._

        val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

        //writes all the tweets from twitter's stream into a directory
        //by default hits the sampled stream and uses "twitterstream" as the directory
        //run in the background using a Future:
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
            tweetStreamToDirUser(bearerToken, queryString = "?usernames=Louis_Tomlinson,BTS_twt,elonmusk,tedcruz&user.fields=public_metrics")
        }
        
        //busy  wait until a file appears in our followers stream directory
        var start = System.currentTimeMillis()
        var filesFoundInDir = false
        while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
            filesFoundInDir = Files.list(Paths.get("followersWed")).findFirst().isPresent()
            Thread.sleep(500)
        }
        if(!filesFoundInDir){
            println("Error: unable to populate follower after 30")
            System.exit(1)
        }

        val firstFollowersDf = spark.read.json("followers")

        val staticDf = spark.read.json("followersWed")

        // val streamDf = spark.readStream.schema(staticDf.schema).json("followers")

        // //Just getting the text
        // staticDf
        //   .select($"data.username", functions.element_at($"data.public_metrics", 1)("followers_count").as("Followers_elon"),
        //   functions.element_at($"data.public_metrics", 1)("tweet_count").as("Tweet_count_elon"),
        //   functions.element_at($"data.public_metrics", 2)("followers_count").as("Followers_bts"),
        //   functions.element_at($"data.public_metrics", 2)("tweet_count").as("Tweet_count_bts"))
        //   .show()

        firstFollowersDf
          .select($"data.username", ($"data.public_metrics.followers_count").as("Followers"), ($"data.public_metrics.Tweet_count").as("Tweet_count"))
          .orderBy("Tweet_count", "Followers")
          .show(20, false)

        //static for wednesday table
        println("***Table for wednesday***")
        staticDf
          .select($"data.username", $"data.public_metrics.followers_count", $"data.public_metrics.Tweet_count")
          .show(20, false)

        staticDf
        .select(functions.element_at($"data", 1)("username").as("Username"),
          functions.element_at($"data.public_metrics", 1)("followers_count").as("Followers"),
          functions.element_at($"data.public_metrics", 1)("tweet_count").as("Tweet_count"))
        .union(
            staticDf
            .select(functions.element_at($"data", 2)("username").as("Username"),
              functions.element_at($"data.public_metrics", 2)("followers_count").as("Followers"),
              functions.element_at($"data.public_metrics", 2)("tweet_count").as("Tweet_count"))
            .union(
                staticDf
                .select(functions.element_at($"data", 3)("username").as("Username"),
                  functions.element_at($"data.public_metrics", 3)("followers_count").as("Followers"),
                  functions.element_at($"data.public_metrics", 3)("tweet_count").as("Tweet_count"))
                .union(
                  staticDf
                  .select(functions.element_at($"data", 4)("username").as("Username"),
                    functions.element_at($"data.public_metrics", 4)("followers_count").as("Followers"),
                    functions.element_at($"data.public_metrics", 4)("tweet_count").as("Tweet_count"))

                )
            )
        )
        .orderBy($"Followers".desc, $"Tweet_count".desc)
        .show(100, false)

    }

    case class Person(_id: String, index: Long, age: Long, eyeColor: String, phone: String, name: Name) {}

    case class Name(first: String, last: String){}

}