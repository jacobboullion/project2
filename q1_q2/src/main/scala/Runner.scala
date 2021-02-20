package q1;

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
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

object Runner {
  def main(args: Array[String]): Unit = {
    //initialize a SparkSession, by convention called spark
    //SparkSession is the entrypoint for a Spark application using Spark SQL
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .master("local[4]")
      .getOrCreate()

    //we want to always add an import here, it enables some syntax and code generation:
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // helloTweetStream(spark)

    // helloTweetStream2(spark)

    filterLocation(spark)

    // q1.filterCountry(spark)

    // q1.filterCountryLang(spark)
  }

  def helloTweetStream(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    //writes all the tweets from twitter's stream into a directory
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir(
        bearerToken,
        queryString =
          "?tweet.fields=geo,lang&place.fields=country&expansions=geo.place_id"
      )
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir =
        Files.list(Paths.get("twitterstream")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds.  Exiting.."
      )
      System.exit(1)
    }

    val staticDf = spark.read.json("twitterstream")

    val streamDf =
      spark.readStream.schema(staticDf.schema).json("twitterstream")

    //Display place names as tweets occur there
    streamDf
      .filter(!functions.isnull($"includes.places"))
      .select(
        functions.element_at($"includes.places", 1)("country").as("Country"),
        ($"data.lang").as("Language")
      )
      .groupBy($"Country", $"Language")
      .count()
      .sort(functions.desc("count"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  def tweetStreamToDir(
      bearerToken: String,
      dirname: String = "twitterstream",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {
    //a decent chunk of boilerplate -- from twitter docs/tutorial
    //sets up the request we're going to be sending to Twitter
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/sample/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    //set up the authorization for this request, using our bearer token
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("twitterstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("twitterstream.tmp"),
            Paths.get(
              s"$dirname/twitterstream-$millis-${lineNumber / linesPerFile}"
            )
          )
          fileWriter = new PrintWriter(Paths.get("twitterstream.tmp").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }

  def filterLocation(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("q2_f")

    df
      .filter(!functions.isnull($"includes.places"))
      .select(
        functions.element_at($"includes.places", 1)("full_name").as("Place")
      )
      .groupBy("Place")
      .count()
      .sort(functions.desc("count"))
      .show(20, false)
  }

  def helloTweetStream2(spark: SparkSession): Unit = {
    import spark.implicits._

    val bearerToken = System.getenv(("TWITTER_BEARER_TOKEN"))

    //writes all the tweets from twitter's stream into a directory
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetStreamToDir2(
        bearerToken,
        queryString =
          "?tweet.fields=geo,lang&place.fields=country&expansions=geo.place_id"
      )
    }

    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir = Files.list(Paths.get("q2_f")).findFirst().isPresent()
      Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds.  Exiting.."
      )
      System.exit(1)
    }

    val staticDf = spark.read.json("q2_f")

    val streamDf =
      spark.readStream.schema(staticDf.schema).json("q2_f")

    //Display place names as tweets occur there
    streamDf
      .filter(!functions.isnull($"includes.places"))
      .select(
        functions.element_at($"includes.places", 1)("full_name").as("Place")
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

  }

  def tweetStreamToDir2(
      bearerToken: String,
      dirname: String = "q2_f",
      linesPerFile: Int = 1000,
      queryString: String = ""
  ) = {
    //a decent chunk of boilerplate -- from twitter docs/tutorial
    //sets up the request we're going to be sending to Twitter
    val httpClient = HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
    val uriBuilder: URIBuilder = new URIBuilder(
      s"https://api.twitter.com/2/tweets/sample/stream$queryString"
    )
    val httpGet = new HttpGet(uriBuilder.build())
    //set up the authorization for this request, using our bearer token
    httpGet.setHeader("Authorization", s"Bearer $bearerToken")
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity()
    if (null != entity) {
      val reader = new BufferedReader(
        new InputStreamReader(entity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("q2_f.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("q2_f.tmp"),
            Paths.get(
              s"$dirname/q2_f-$millis-${lineNumber / linesPerFile}"
            )
          )
          fileWriter = new PrintWriter(Paths.get("q2_f.tmp").toFile)
        }

        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }

}
