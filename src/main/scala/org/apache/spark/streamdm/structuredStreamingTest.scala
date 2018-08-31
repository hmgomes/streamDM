package org.apache.spark.streamdm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier


object structuredStreamingTest {

  def main(args : Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    streamingPredictionsFromFile(smallDfPath = "/Users/heitor/Desktop/data/covtype_small.csv")
//    streamingPredictionsFromNetwork()
  }

  def streamingPredictionsFromNetwork(): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("StreamingPredictions")
        .config("spark.master", "local[4]")
        .getOrCreate()

    //
    val irisDataFrame : DataFrame = spark.read.option("header", false).option("inferSchema", true).csv("/Users/heitor/Desktop/workspace/StreamDM-main/streamDM/data/iris.csv")
        println("Number of instances: " + irisDataFrame.count())
        println("head = " + irisDataFrame.head())
        irisDataFrame.show()

    irisDataFrame.printSchema()

    val Array(trainData, testData) = irisDataFrame.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()


    val inputCols = trainData.columns.filter(_ != "_c4")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("X")
    val assembledTrainData = assembler.transform(trainData)
    assembledTrainData.select("X").show(truncate = false)

    //    import spark.implicits._

    val rfClassifier = new RandomForestClassifier().setNumTrees(10).setSeed(1).setLabelCol("_c4").setFeaturesCol("X").setPredictionCol("prediction")
    val model = rfClassifier.fit(assembledTrainData)

    //    HERE STARTS THE STREAMING PREDICTIONS PART!
    // Running netcat on port 9999: nc -lk 9999

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Query the input line to create an instance in the expected format.
    // If the input format does not contain at least 4 comma separated numerical values something unexpected might happen (like an exception).
    val rows = lines.selectExpr("cast(split(value, ',')[0]  as double) as _c0", "cast(split(value, ',')[1] as double) as _c1",
      "cast(split(value, ',')[2] as double) as _c2", "cast(split(value, ',')[3] as double) as _c3") // "cast(split(value, ',')[4] as double) as _c4"

    // Use the "assembler" created before to create the X from the Dataset "rows".
    val assembledRows = assembler.transform(rows)
    // Obtain the predictions from the
    val predictedAssembledRows = model.transform(assembledRows)

    // Start running the query that prints the predictions to the console
    val query = predictedAssembledRows
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def streamingPredictionsFromFile(smallDfPath: String = "/Users/heitor/Desktop/data/iris_small.csv",
                                   streamsPath: String = "/Users/heitor/Desktop/data/stream"): Unit = {
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("StreamingPredictions")
        .config("spark.master", "local[4]")
        .getOrCreate()

    // Use a very small dataset to infer the Schema, optionally could have created the schema manually
    val irisDataFrame : DataFrame = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(smallDfPath )

    irisDataFrame.printSchema()

    val inputCols = irisDataFrame.columns.filter(_ != "class")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("X")
    val assembledTrainData = assembler.transform(irisDataFrame)
    assembledTrainData.select("X").show(truncate = false)

    //    import spark.implicits._

    val rfClassifier = new RandomForestClassifier().setNumTrees(10).setSeed(1).setLabelCol("class").setFeaturesCol("X").setPredictionCol("prediction")
    val model = rfClassifier.fit(assembledTrainData)

    //    HERE STARTS THE STREAMING PREDICTIONS PART!

    val rows : DataFrame = spark.readStream
        .format("csv")
        .schema(irisDataFrame.schema)
        .option("maxFilesPerTrigger", 1)
        .load(streamsPath)

    // Use the "assembler" created before to create the X from the Dataset "rows".
    val assembledRows = assembler.transform(rows)
    // Obtain the predictions from the
    val predictedAssembledRows = model.transform(assembledRows)

    // Start running the query that prints the predictions to the console
    val query = predictedAssembledRows
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

  // Must read the Schema from a file
  // It can be a short version of the dataset
}
