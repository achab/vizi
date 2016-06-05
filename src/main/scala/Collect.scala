import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable._
import Utils.{configureTwitterCredentials}

object Collect {
  private var numTweetsCollected = 0L
  private var numTweetsToCollect = 1000

  def main(args: Array[String]) {

    val sparkHome = "/Users/massil/Programmation/spark/spark" // Location of the Spark directory
    val sparkUrl = "local[4]" // URL of the Spark cluster
    val jarFile = "target/scala-2.10/vizi_2.10-0.1-SNAPSHOT.jar" // Location of the required JAR files
    // val checkpointDir = "/Users/massil/Desktop/tmp" // HDFS directory for checkpointing

    // Process program arguments and set properties
    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<filterTag> <numTweetsToCollect>")
      System.exit(1)
    }

    val filterTag = args(0)
    val numTweetsToCollect = args(1).toInt
    val outputDirectory = filterTag + "/"
    //val Array(filterTag, outputDirectory, Utils.IntParam(numTweetsToCollect)) = Utils.parseCommandLine()

    // Configure Twitter credentials using twitter.txt
    configureTwitterCredentials()

    // Set StreamingContext
    val ssc = new StreamingContext(sparkUrl, "Vizi", Seconds(1), sparkHome, Seq(jarFile))
    val filters = Array(filterTag)
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val statuses = tweets.map(status => status.getText())
    val words = statuses.flatMap(status => status.split(" "))
    val hashtags = words.filter(word => word.startsWith("#"))
    val tagCounts = hashtags.window(Minutes(10), Seconds(1)).countByValue()

    /*
    val counts = hashtags.map(tag => (tag, 1))
                         .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 2), Seconds(1))
    val sortedCounts = counts.map { case(tag, count) => (count, tag) }
                             .transform(rdd => rdd.sortByKey(false))
    sortedCounts.foreach(rdd =>
      println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))

    val arr = new ArrayBuffer[String]();
    words.foreachRDD {
        arr ++= _.collect() //you can now put it in an array or d w/e you want with it
        // ...
    }
    */

    // statuses.saveAsTextFiles("output/statuses")
    statuses.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(100)
        outputRDD.saveAsTextFile(outputDirectory + "output_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    //ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()



  }
}
