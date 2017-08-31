package com.pawelgebal

import java.io.InputStream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Framing, Sink, StreamConverters}
import akka.util.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object HdfsTextFileStreamer {

  private val Delimiter = ByteString("\n")
  private val MaxLineLength = 500
  private val PrintTimeout = 5.seconds

  implicit private val system = ActorSystem()
  implicit private val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val (hadoopUserName, filePath) = getUserNameAndFilePath(args)
    System.setProperty("HADOOP_USER_NAME", hadoopUserName)

    val hdfs = getHdfsFileSystemForFile(filePath)
    val textStream = hdfs.open(new Path(filePath))

    await(printTextStream(textStream))

    materializer.shutdown()
    await(system.terminate())
    hdfs.close()
  }

  private def getUserNameAndFilePath(args: Array[String]) = {
    require(args.length == 2)
    val Array(hadoopUserName, filePath) = args
    (hadoopUserName, filePath)
  }

  private def printTextStream(textStream: InputStream): Future[Done] =
    StreamConverters
      .fromInputStream(() => textStream)
      .via(Framing.delimiter(Delimiter, MaxLineLength, allowTruncation = true))
      .map(_.utf8String)
      .runWith(Sink.foreach(println))

  private def getHdfsFileSystemForFile(filePath: String): FileSystem = {
    val host = hdfsHostForFile(filePath)

    val configuration = new Configuration()
    configuration.set("fs.defaultFS", host)

    FileSystem.get(configuration)
  }

  private def hdfsHostForFile(filePath: String) = {
    val pattern = "(hdfs://[^/]*)/.*".r
    filePath match {
      case pattern(hdfsHost) => hdfsHost
    }
  }

  private def await[A](f: Future[A]): A = Await.result(f, PrintTimeout)
}
