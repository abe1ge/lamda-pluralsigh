package streaming.StreamingJob


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 02/05/2017.
  */
object LocalStream {
  def main(args: Array[String]): Unit = {

    //turn of the logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //setup spark context
    val conf = new SparkConf().setAppName("Streamer").setMaster("local[*]")
    //create spark streaming context with 2 second intervials
    val ssc = new StreamingContext(conf, Seconds(2))

    val winLen = Seconds(10)
    val slideInt = Seconds(2)

    //file path reading from
    val lines = ssc.textFileStream("file:///C:/Users/Administrator/Documents/abel/bigdata/streaming")

    //split on "," and take the third item
    val cost = lines.map(x => x.split(",")).map(x => x(2).toInt)

    val avg = cost.reduceByWindow(_ + _, winLen, slideInt).map(x => x/20)

    //print result
    avg.print()

    //start streaming
    ssc.start()
    //don't stop untill told to
    ssc.awaitTermination()
  }
}
