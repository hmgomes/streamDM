/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.streamdm.evaluation

import java.io.Serializable

import com.github.javacliparser.FlagOption
import org.apache.spark.internal.Logging
import com.github.javacliparser.FloatOption
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * The basic classification evaluator output metrics for both binary and
 * multiclass problems.
 * Input: testing Examples and doubles predicted by the learners.
 * Output:
 * (Binary) Accuracy, Recall, Precision, Fbeta-score,
 * Specificity, Confusion Matrix
 * (Multiclass) Accuracy, Recall-macro-avg, Precision-macro-avg,
 * Fbeta-score-macro-avg, Per class recall and precision (Optional),
 * Confusion Matrix (Optional)
 *
 * <p>It uses the following options:
 * <ul>
 *  <li> beta (<b>-b</b>), representing the beta value for f-score calculation
 *  <li> supressPerClassMetrics (<b>-c</b>), when true prevent output of recall and precision per class
 *  <li> supressConfusionMatrix (<b>-m</b>), when true prevent output of the confusion matrix
 * </ul>
 */
class BasicClassificationEvaluator extends Evaluator with Logging {

  val betaOption = new FloatOption("beta", 'b',
    "Beta value for fbeta-score calculation.", 1.0, Double.MinValue, Double.MaxValue)

  val suppressPerClassMetricsOption = new FlagOption("suppressPerClassMetrics",
    'c', "Do not output the per class precision and recall for multi-class problems.")

  val suppressConfusionMatrixOption = new FlagOption("suppressConfusionMatrix",
    'm', "Do not output the confusion matrix.")

  override def setSchema(schema: StructType) = {
    this.schema = schema
  }

  // This stores the time in ns for the original call to addResult
  var startTime: Long = 0

  /**
    * Process the result of a predicted stream of Examples and Doubles.
    * The second value of the tuple (Double) contains the predicted value.
    * The first value contain the original Example.
    *
    * @param stream the input stream containing "class" (ground truth) and "predict" column
    * @return a stream of String with the processed evaluation
    */
  override def addResult(stream: DataFrame): DataFrame = {
    this.startTime = System.nanoTime()

    val numClasses = this.schema("class").metadata.getLong("num_class").toInt //exampleLearnerSpecification.outputFeatureSpecification(0).range
    var resultsStream: DataFrame = stream
    if(numClasses == 2) {
      val confusionMatrix = ConfusionMatrix.computeMatrix(stream)
      resultsStream = calculateMetricsBinary(confusionMatrix)
    }
    else {
      // TODO: implement the multi-class calculations using SQL
//      val confusionMatrixMultiClass = ConfusionMatrixMultiClass.computeMatrix(stream, numClasses)
//      resultsStream = calculateMetricsMultiClass(confusionMatrixMultiClass)
    }

// TODO: Perhaps keep the statistics in the predicted column metadata
    // Set the prediction results as metadata of the original DataFrame in the predicted column...
//    val metadataEvaluationResults = new MetadataBuilder().putString("evaluation", evaluationResults).build()
//
//    val newColumnPredicted = stream.col("predicted").as("predicted", metadataEvaluationResults)
//
//    val streamUpdatedResults = stream.withColumn("predicted", newColumnPredicted)

    // Should reduce the input to a table with only evaluation
    resultsStream
  }

  /**
    * Calculate several evaluation metrics for binary classification based
    * on a confusion matrix
    * @param aggConfusionMatrixStream the binary confusion matrix
    * @return one string containing the metrics
    */
  def calculateMetricsBinary(aggConfusionMatrixStream : DataFrame ): DataFrame = {
//    val timeTaken = (System.nanoTime() - startTime)/1e+9 // divide by 1e+9 to obtain the estimation in seconds.
//    val accuracy = (confMat{"tp"}+confMat{"tn"})/(confMat{"tp"}+confMat{"tn"}+confMat{"fp"}+confMat{"fn"})
//    val recall = confMat{"tp"} / (confMat{"tp"}+confMat{"fn"})
//    val precision = confMat{"tp"} / (confMat{"tp"}+confMat{"fp"})
//    val specificity = confMat{"tn"} / (confMat{"tn"}+confMat{"fp"})
//    val f_beta_score = (1 + scala.math.pow(this.betaOption.getValue(),2)) * ((precision * recall) /
//      ((scala.math.pow(this.betaOption.getValue(),2) * precision) + recall))
//
//     "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.0f,%.0f,%.0f,%.0f".format(timeTaken, accuracy, recall, precision, f_beta_score, specificity,
//        confMat{"tp"}, confMat{"fn"}, confMat{"fp"}, confMat{"tn"})

    val sparkSession = SparkSession.builder().getOrCreate()

    aggConfusionMatrixStream.createOrReplaceTempView("agg_confusion_matrix_stream")

    // TODO: Missing the time taken estimation
    val resultsStream = sparkSession.sql("SELECT " +
      "(TN_sum + TP_sum) / (TP_sum + TN_sum + FP_sum + FN_sum) as accuracy, " +
      "TP_sum / (TP_sum + FN_sum) as recall, " +
      "TP_sum / (TP_sum + FP_sum) as precision, " +
      "TN_sum / (TN_sum + FP_sum) as specificity, " +
      "TP_sum as TP, TN_sum as TN, FP_sum as FP, FN_sum as FN FROM agg_confusion_matrix_stream")

    resultsStream
  }

  /**
    * Calculate several evaluation metrics for multiclass classification based
    * on a confusion matrix
    * @param confMat the multiclass confusion matrix
    * @return one string containing the metrics
    */
  def calculateMetricsMultiClass(confMat : Map[(Int, Int), Double]): String = ???
//  {
//    val timeTaken = (System.nanoTime() - startTime)/1e+9 // divide by 1e+9 to obtain the estimation in seconds.
//    val numClasses = this.schema("class").metadata.getLong("num_class").toInt
//    val instancesSeenBatch = confMat.values.reduce(_+_)
//    val accuracy = confMat.filter(t => t._1._1 == t._1._2).values.reduce(_+_) / instancesSeenBatch
//    val recallPerClass = calculateRecallMultiClass(confMat)
//    val precisionPerClass = calculatePrecisionMultiClass(confMat)
//    // Ignore NaN values while calculating the macro average recall and precision.
//    val recallMacro = recallPerClass.filter(!_.isNaN()).reduce((v,k) => v + k) / (numClasses - recallPerClass.count(_.isNaN))
//    val precisionMacro = precisionPerClass.filter(!_.isNaN()).reduce((v,k) => v + k) / (numClasses - precisionPerClass.count(_.isNaN))
//    val squaredBeta = scala.math.pow(this.betaOption.getValue(),2)
//    // Avoid division by zero in fscore
//    val fscoreMacro = if ((squaredBeta * precisionMacro + recallMacro) != 0) ((squaredBeta +1) * precisionMacro * recallMacro) / (squaredBeta * precisionMacro + recallMacro) else 0.0
//
//    var output = "%.3f,%.3f,%.3f,%.3f,%.3f,".format(timeTaken, accuracy, recallMacro, precisionMacro, fscoreMacro)
//
//    if(!this.supressPerClassMetricsOption.isSet()) {
//      val strRecallPerClass = recallPerClass.foldLeft("")((r : String, c : Double) => r+"%.4f,".format(c))
//      val strPrecisionPerClass = precisionPerClass.foldLeft("")((r : String, c : Double) => r+"%.4f,".format(c))
//      output += "%s%s".format(strRecallPerClass, strPrecisionPerClass)
//    }
//
//    if(!this.supressConfusionMatrixOption.isSet()) {
//      output += "%s".format(confMat.map( x => "(%d %d)=%.1f".format(x._1._1, x._1._2, x._2) ).mkString(","))
//    }
//    output
//  }

  /***
    * Calculates recall for each class label.
    * @param confMat confusion matrix (predicted groundtruth) => instance counter
    * @return array containing the recall for each class label
    */
  def calculateRecallMultiClass(confMat : Map[(Int, Int), Double]): Array[Double] = ???
//  {
//    val numClasses = this.schema("class").metadata.getLong("num_class").toInt
//    var recallPerClassIndex = Array.fill(numClasses){0.0}
//
//    for(groundTruth <- Range(0, numClasses)) {
//      var TPi = 0.0
//      var FNi = 0.0
//      for(predicted <- Range(0, numClasses)) {
//        if(predicted == groundTruth) {
//          TPi += confMat( (predicted, groundTruth) )
//        }
//        else {
//          FNi += confMat( (predicted, groundTruth) )
//        }
//      }
//      recallPerClassIndex(groundTruth) = TPi / (TPi + FNi)
//    }
//    recallPerClassIndex
//  }

  /***
    * Calculates precision for each class label.
    * @param confMat confusion matrix (predicted groundtruth) => instance counter
    * @return array containing the precision for each class label
    */
  def calculatePrecisionMultiClass(confMat : Map[(Int, Int), Double]): Array[Double] = ???
//  {
//    val numClasses = this.schema("class").metadata.getLong("num_class").toInt
//    var precisionPerClassIndex = Array.fill(numClasses){0.0}
//
//    for(predicted <- Range(0, numClasses)) {
//      var TPi = 0.0
//      var FPi = 0.0
//
//      for(groundTruth <- Range(0, numClasses)) {
//        if(predicted == groundTruth) {
//          TPi += confMat( (predicted, groundTruth) )
//        }
//        else {
//          FPi += confMat( (predicted, groundTruth) )
//        }
//      }
//      precisionPerClassIndex(predicted) = TPi / (TPi + FPi)
//    }
//    precisionPerClassIndex
//  }

  /**
    * The header changes according to the number of classes.
    * @return a String representing the measurements header
    */
  override def header(): String = ???
//  {
//    val numClasses = this.schema("class").metadata.getLong("num_class").toInt
//    if(numClasses == 2) {
//      "Runtime,Accuracy,Recall,Precision,F(beta=%.1f)-score,Specificity,TP,FN,FP,TN".format(this.betaOption.getValue())
//    }
//    else {
//      var output = "Runtime,Accuracy,Recall-avg-macro,Precision-avg-macro,F(beta=%.1f)-score-avg-macro,".format(this.betaOption.getValue())
//      if(!this.supressPerClassMetricsOption.isSet()) {
//        val perClassRecall = Range(0, numClasses).foldLeft("")( (s,i) => s + "Recall(" + i + "),")
//        val perClassPrecision = Range(0, numClasses).foldLeft("")( (s,i) => s + "Precision(" + i + "),")
//        output += perClassRecall + perClassPrecision
//      }
//      if(!this.supressConfusionMatrixOption.isSet()) {
//        output += "ConfusionMatrix(predicted ground-truth)"
//      }
//      output
//    }
//  }
}

/**
 * Helper class for computing the confusion matrix for binary classification.
 */
object ConfusionMatrix extends Serializable with Logging {
  def computeMatrix(stream: DataFrame): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()

    stream.createOrReplaceTempView("stream")

    val resultsStream =
      sparkSession.sql("SELECT " +
        "CASE WHEN (class = 1 AND predicted = 1 AND class = predicted) THEN 1 ELSE 0 END AS TP, " +
        "CASE WHEN (class = 0 AND predicted = 0 AND class = predicted) THEN 1 ELSE 0 END AS TN, " +
        "CASE WHEN (class = 0 AND predicted = 1) THEN 1 ELSE 0 END AS FP, " +
        "CASE WHEN (class = 1 AND predicted = 0) THEN 1 ELSE 0 END AS FN " +
      "FROM stream")

    resultsStream.createOrReplaceTempView("results_stream")

    val aggResultsStream =
      sparkSession.sql("SELECT sum(TP) as TP_sum, sum(TN) AS TN_sum, sum(FP) as FP_sum, sum(FN) as FN_sum " +
        "FROM results_stream")

    aggResultsStream
  }
}

/**
  * Helper class for computing the confusion matrix for multi-class classification.
  */
object ConfusionMatrixMultiClass extends Serializable {
  def confusion(x: (Double, Double), numClasses: Int): Map[(Int, Int), Double] = ???
//  {
//    var outMat: Map[(Int, Int), Double] = Map()
//    for(predicted <- Range(0, numClasses)) {
//      for(groundTruth <- Range(0, numClasses)) {
//        if (x._2 == predicted && x._1 == groundTruth)
//          outMat += (predicted, groundTruth) -> 1.0
//        else
//          outMat += (predicted, groundTruth) -> 0.0
//      }
//    }
//    outMat
//  }

  def computeMatrix(stream: DataFrame, numClasses: Int): Map[(Int, Int), Double] = ???
//  {
//    val sparkSession = SparkSession.builder().getOrCreate()
//    import sparkSession.implicits._
//
//    stream.map(x => confusion((x.getDouble(x.fieldIndex("class")), x.getDouble(x.fieldIndex("predicted"))),
//      numClasses)).reduce((x, y) => y ++ x.map { case (k, v) => k -> (v + y.getOrElse(k, 0.0)) })
//  }
}
