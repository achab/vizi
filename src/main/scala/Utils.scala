import com.fasterxml.jackson.annotation.ObjectIdGenerators.None
import net.liftweb.json.JsonParser.ParseException
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

  def getAuth = {
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }

  /*
  val THE_OPTIONS = {
    val options = new Options()
    options
  }

  def parseCommandLine(args: Array[String]) = {
    val parser = new PosixParser
    try {
      val cl = parser.parse(THE_OPTIONS, args)
      cl.getArgList.toArray
    } catch {
      case e: ParseException =>
        System.err.println("Parsing failed. Reason: " + e.getMessage)
        System.exit(1)
    }
  }
  */

  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }
}
