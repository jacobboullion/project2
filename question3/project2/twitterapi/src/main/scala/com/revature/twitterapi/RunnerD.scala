package com.revature.twitterapi

import org.apache.spark.sql.SparkSession
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.concurrent.Future
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import collection.JavaConversions._


object RunnerD {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("Project2 Spark SQL")
            .master("local[4]")
            .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        // uncomment this line in order to stream down data from Twitter to save in folders
        tweetStream(spark)

        // uncomment this to run the analysis on the saved data 
        //sparkSql(spark)
    
    }

    def tweetStream(spark: SparkSession): Unit = {
        import spark.implicits._

        val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

        tweetStreamDir(bearerToken)

        var start = System.currentTimeMillis()
        var filesFoundInDir = false
        while(!filesFoundInDir && (System.currentTimeMillis()-start) < 30000) {
        filesFoundInDir = Files.list(Paths.get("twitterstream")).findFirst().isPresent()
        Thread.sleep(500)
        }
        if(!filesFoundInDir) {
        println("Error: Unable to populate tweetstream after 30 seconds.  Exiting..")
        System.exit(1)
        }

        val staticDf = spark.read.json("twitterstream")

        staticDf.printSchema()

        val streamDf = spark.readStream.schema(staticDf.schema).json("twitterstream")

        // Used to check that data was streaming down correctly 
        streamDf
            .select($"data.text")
            .writeStream
            .outputMode("append") 
            .format("console")
            .start()
            .awaitTermination()

    }


    def tweetStreamDir(
        bearerToken: String,
        dirname: String = "twitterstream",
        linesPerFile: Int = 10
    ) = {

        val httpClient = HttpClients.custom
            .setDefaultRequestConfig(
                RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
            )
            .build()
            val uriBuilder: URIBuilder = new URIBuilder(
                "https://api.twitter.com/2/tweets/search/stream"
            )
            val httpGet = new HttpGet(uriBuilder.build())
            httpGet.setHeader("Authorization", s"Bearer $bearerToken")
            val response = httpClient.execute(httpGet)
            val entity = response.getEntity()
            if (null != entity) {
                val reader = new BufferedReader(
                    new InputStreamReader(entity.getContent())
                )
               var line = reader.readLine()
               var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
               var lineNumber = 1
               val millis = System.currentTimeMillis()
               while(line != null) {
                   if (lineNumber % linesPerFile == 0) {
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

    // Used this function in order to filter out the hashtags in the tweets and group them by the most common
    def sparkSql(spark: SparkSession): Unit = {
        import spark.implicits._

        val df = spark.read.json("twitterstream")
       
            df
            .select($"data.text")
            .as[String]
            .flatMap(_.split("\\s+"))
            .filter(word => word.startsWith("#"))
            .groupBy("value")           
            .count()    
            .sort(functions.desc("count"))
            .show()

            df.printSchema()
    }

}