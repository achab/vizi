import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
//import org.apache.commons.cli._

object Utils {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.streaming.NetworkInputTracker").setLevel(Level.INFO)

  /** Configures the Oauth Credentials for accessing Twitter */
  def configureTwitterCredentials() {
    val file = new File("src/main/resources/twitter.txt")
    if (!file.exists) {
      throw new Exception("Could not find configuration file " + file)
    }
    val lines = Source.fromFile(file.toString).getLines.filter(_.trim.size > 0).toSeq
    val pairs = lines.map(line => {
      val splits = line.split("=")
      if (splits.size != 2) {
        throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]")
      }
      (splits(0).trim(), splits(1).trim())
    })
    val map = new HashMap[String, String] ++= pairs
    val configKeys = Seq("consumerKey", "consumerSecret", "accessToken", "accessTokenSecret")
    println("Configuring Twitter OAuth")
    configKeys.foreach(key => {
        if (!map.contains(key)) {
          throw new Exception("Error setting OAuth authenticaion - value for " + key + " not found")
        }
        val fullKey = "twitter4j.oauth." + key
        System.setProperty(fullKey, map(key))
        println("\tProperty " + fullKey + " set as " + map(key))
    })
    println()
  }

/*
  case class Config(filterTag: String = "", numTweetsToCollect: Int = 1000, outputDirectory: String = "output/")

  def parseCommandLine(args: Array[String]) = {
    val parser = new OptionParser[Config]("scopt")
    val cl = parser.parse(THE_OPTIONS, args)
    cl.getArgList.toArray
  }


  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }

  */

}
