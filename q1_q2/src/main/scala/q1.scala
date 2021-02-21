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
import org.apache.spark.sql.functions.split

object q1 {

  def filterCountry(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("twitterstream")

    df
      .filter(!functions.isnull($"includes.places"))
      .select(
        functions.element_at($"includes.places", 2)("full_name").as("Place")
      )
      .groupBy("Country")
      .count()
      .sort(functions.desc("count"))
      .show()
  }

  def filterCountryLang(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("twitterstream")

    df
      .filter(!functions.isnull($"includes.places") && !($"data.lang".contains("und")))
      .select(
        functions.element_at($"includes.places", 1)("country").as("Country"),
        ($"data.lang").as("Language")
      )
      .groupBy($"Country", $"Language")
      .count()
      .sort(functions.desc("Country"))
      .show()
  }
}
