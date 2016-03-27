import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.mutable._
import TutorialHelper._

object Tutorial {
  def main(args: Array[String]) {

    // Location of the Spark directory
    val sparkHome = "/Users/massil/Programmation/spark/spark"

    // URL of the Spark cluster
    val sparkUrl = "local[4]"

    // Location of the required JAR files
    val jarFile = "target/scala-2.10/tutorial_2.10-0.1-SNAPSHOT.jar"

    // HDFS directory for checkpointing
    val checkpointDir = "/Users/massil/Desktop/tmp"
    // val checkpointDir = TutorialHelper.getHdfsUrl() + "/checkpoint/"

    // Configure Twitter credentials using twitter.txt
    TutorialHelper.configureTwitterCredentials()

    // Set StreamingContext
    val ssc = new StreamingContext(sparkUrl, "Tutorial", Seconds(1), sparkHome, Seq(jarFile))

    val filters = Array("#Pakistan")
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val statuses = tweets.map(status => status.getText())
    val words = statuses.flatMap(status => status.split(" "))
    val hashtags = words.filter(word => word.startsWith("Eiffel"))

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

    statuses.saveAsTextFiles("output/statuses")


    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()



  }
}
