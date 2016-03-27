import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
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

    val filters = Array("#SOSPascal", "#TontonsFlingueurs")
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    statuses.print()

    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()



  }
}
