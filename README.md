# PROJECT 2

## Project Description

Project 2’s analysis consists of using big data tools to answer questions created by the team on streaming Twitter data. The following series of questions asked were answered using Structured Streaming methods and running analysis using DataFrames and DataSets. Determining what type of streaming data to gather from Twitter was based on the context of each question, whether that being using the Sampled Stream, Filtered Stream, or User Lookup. The output and analysis of each question was saved as a jarfile, and the final presentation presented included in depth analysis of the result findings. The questions follow: 1) What is the most currently referenced country?  Does that change based on the origin country of the tweet?  How much conversation about countries on twitter is done by residents vs. foreigners? 2) What individual location is referenced the most in a day? (Monday-Sunday) 3) What other hashtags are associated with the America hashtag? 4) Which country is talking about soccer/ fútbol more out of the countries that use these terms? 5) Does the activity on a Twitter page translate to more followers?

## Technologies Used

* SparkSQL 
* DataFrames/Datasets
* Structured Streaming in sparkSql
* Twitter API
* Scala

## Features

List of features ready and TODOs for future development
* Loads data from twitter API
* Puts that data into the specified directories
* Queries are run on the dataframes/datasets to get the results for each question

To-do list:
* Adjust the twitter API to get more specfic tweets
* Simplify number of Queries

## Getting Started
   
(include git clone command)
(include all environment setup steps)
* git clone the repo
* install scala, spark, and sparkSQL
* create a twitter developer profile to get the information on twitter
* You can use vscode or terminal in order to modify and run the code
* Once your connected you can start using the queries in the provided code
* The Twitter information that is used as input to the programs can be found at: https://developer.twitter.com/en

## Usage

> You can run the queries like, 
>       staticDf
          .filter(!functions.isnull($"includes.places"))
          .select(functions.element_at($"includes.places", 1)("country").as("Country"))
          .groupBy("Country")
          .count()
          .sort(functions.desc("count"))
          .show()
   in order to go through static data and
>  streamDf
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
   in order to go through streaming data.

## Contributors
Jacob Boullion, Allie Burkeen, Delaney Lekien
## License
> No lincense
