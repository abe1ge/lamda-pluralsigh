package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by work on 28/04/2017.
  */
object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {
    var checkpointDirectory = ""
    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)

    // check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "winutils.exe") // required for winutils
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///C:/Users/work/Documents/plursight/applyingLambdaArchitecture/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/temp/"
    }else{
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sQLContext = SQLContext.getOrCreate(sc)
    sQLContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc : () => StreamingContext = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
