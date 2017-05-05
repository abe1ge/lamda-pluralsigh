package mlLib

import java.lang.management.ManagementFactory

import domain.{Movie, User}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 04/05/2017.
  */
object MlMovie {
  def main(args: Array[String]): Unit = {

    //turn of the logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // setup spark context
    val conf = new SparkConf().setAppName("Machine Learning")

    // check if running from IDE
    val isIDE = {
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    //where to look for files
    var userSF   = ""
    var moviesSF = ""
    var ratingSF = ""
    if (isIDE) {
      conf.setMaster("local[*]")

      userSF    = "sparklambda/src/main/resources/users.dat"
      moviesSF = "sparklambda/src/main/resources/movies.dat"
      ratingSF = "sparklambda/src/main/resources/ratings.dat"
    }else{
      userSF = "movies/users.dat"
      moviesSF = "movies/movies.dat"
      ratingSF = "movies/ratings.dat"
    }

    val sc = new SparkContext(conf)

    // where it begins

    val userData = sc.textFile(userSF).map(x => x.split("::"))
    val moviesData = sc.textFile(moviesSF).map(x => x.split("::"))
    val ratingData = sc.textFile(ratingSF).map(x => x.split("::"))

    val userRDD = userData.map(x => User(x(0).toInt, x(1).toString, x(2).toInt, x(3).toInt, x(4).toString))
    val movieRDD = moviesData.map(x => Movie(x(0).toInt, x(1)))
    val ratingRDD = ratingData.map(x => Rating(x(0).toInt, x(1).toInt, x(2).toFloat))

    // Randomly split the data into training and test and cache these new datasets
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingRatings = splits(0).cache()
    val testRatings = splits(1).cache()

    // Build an Alternating Least Squares model
    // Here we’ve used the settings of rank = 10 and iterations = 10
    val model = new ALS().setRank(10).setIterations(10).run(trainingRatings)

    //Create a new RDD of each userID and movieID in the testRatings dataset, but without any ratings filled in – an empty matrix
    val blankMovieUserRDD = testRatings.map {
      case Rating(user, movie, rating) => (user,movie)
    }

    // Use the predict() method with your model on the empty RDD to make a predicted rating for each userID/movieID combination
    val predictionsForTest = model.predict(blankMovieUserRDD)
    predictionsForTest.take(5).foreach(println)


  }

}
