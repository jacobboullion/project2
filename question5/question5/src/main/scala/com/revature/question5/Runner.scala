package com.revature.question5

import org.apache.spark.sql.SparkSession
import java.nio.file.Files
import java.nio.file.Paths
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.impl.client.HttpClients
import scala.concurrent.Future
import org.apache.spark.sql.functions

object Runner {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
          .appName("Followers")
          .master("local[4]")
          .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        
    }


    def followerStream(spark: SparkSession): Unit = {
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
        streamDf
          .filter(!functions.isnull($"includes.places"))
          .select(functions.element_at($"includes.places", 1)("full_name").as("Place"), ($"data.text").as("Tweet"))
          .writeStream
          .outputMode("append")
          .format("console")
          .option("truncate", false)
          .start()
          .awaitTermination()

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

        // streamDf
        //   .select($"data.text")
        //   .as[String]
        //   .flatMap(text => {text match {
        //       case pattern(handle) => {Some(handle)}
        //       case notFound => None
        //   }})
        //   .groupBy("value")
        //   .count()
        //   .sort(functions.desc("count"))
        //   .writeStream
        //   .outputMode("complete")
        //   .format("console")
        //   .start()
        //   .awaitTermination()

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
    
}