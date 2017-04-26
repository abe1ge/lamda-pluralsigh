package batch

import java.lang.management.ManagementFactory
import org.apache.spark.{SparkContext, SparkConf}
import domain._
/**
  * Created by work on 26/04/2017.
  */
object BatchJob {
  def main(args: Array[String]): Unit = {

    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // check if running from IDE
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "C:\\winutils") // required for winutils
      conf.setMaster("local[*]")
    }

    // setup spark
    val sc = new SparkContext(conf)

    // initialize input RDD
    val sourceFile = "file:///C:/Users/work/Documents/plursight/applyingLambdaArchitecture/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
      Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1) , record(2), record(3), record(4),record(5),record(6)))
      else
        None
    }
    

    val keyedByProduct = inputRDD.keyBy(x => (x.product, x.timestamp_hour)).cache()

    val visitorsByProduct = keyedByProduct
      .mapValues( x => x.visitor)
      .distinct()
      .countByKey()

    val activityByProduct = keyedByProduct
      .mapValues( x =>
      x.action match {
        case "purchase" => (1, 0, 0)
        case "add_to_cart" => (0, 1, 0)
        case "page_view" => (0, 0, 1)
      })
      .reduceByKey((a, b) => (a._1 + b._1,a._2 + b._2, a._3 + b._3))

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
