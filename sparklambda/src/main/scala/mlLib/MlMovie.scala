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

    ratingRDD.take(10).foreach(println)
    // Randomly split the data into training and test and cache these new datasets
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2), 0L)
    val trainingRatings = splits(0).cache()
    val testRatings = splits(1).cache()


    // Build an Alternating Least Squares model
    // Here we’ve used the settings of rank = 10 and iterations = 10
    val model = new ALS().setRank(10).setIterations(10).run(trainingRatings)

    //Create a new RDD of each userID and movieID in the testRatings dataset, but without any ratings filled in – an empty matrix
    println("Testing")
    testRatings.take(5).foreach(println)
    val blankMovieUserRDD = testRatings.map {
      case Rating(user, movie, rating) => (user,movie)
    }
    blankMovieUserRDD.take(5).foreach(println)


    // Use the predict() method with your model on the empty RDD to make a predicted rating for each userID/movieID combination
    val predictionsForTest = model.predict(blankMovieUserRDD)
    println("Pridiction")
    predictionsForTest.take(5).foreach(println)

    // Rounding the numbers
    val predictionsForTestRound = predictionsForTest.map(x => (x.user, x.rating.round, x.product))
    predictionsForTestRound.take(10).foreach(println)

    // Compare the ratings you already had in the test rating dataset to what you predicted to measure the accuracy

    //creating the key value pairs needed for the join then join these two datasets
    val testKeys = testRatings.map{
      case Rating(user, movie, rating) => ((user, movie), rating)
    }

    val predictionKeys = predictionsForTest.map{
      case Rating(user, movie, rating) => ((user, movie), rating.round)
    }

    val joinedRDD = testKeys.join(predictionKeys)

    val MAE = joinedRDD.map{
      case ((user, product), (testRating, predRating)) =>
        val err = testRating - predRating
        Math.abs(err)
    }.mean()

    println("\n Accuracy for first prediciton is: " + MAE)

    //Find any false positives (where the predicted is high but the actual is low)
    val falsePositives = joinedRDD.filter{
      case ((user,product), (testRating,predRating)) =>
        testRating <= 1 && predRating >= 4
    }

    println("Number of False Positives: "+falsePositives.count())

    // Top recommended movies to a user the in built recommendProduct() method
    // getting the top 10 recommended products for the user with ID 222
    // it returns the ID of the movie and the stright of the rating
    val topRecs = model.recommendProducts(222, 10)
    println("Recomended movies for user ID 222")
    //topRecs.foreach(println)


    val movieTitle = moviesData.map(x => (x(0), x(1))).collectAsMap()
    topRecs.map(x => (movieTitle(x.product.toString), x.rating)).foreach(println)
  }

}
