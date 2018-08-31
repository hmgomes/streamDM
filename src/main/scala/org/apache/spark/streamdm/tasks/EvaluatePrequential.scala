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

package org.apache.spark.streamdm.tasks

import com.github.javacliparser._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streamdm.classifiers._
import org.apache.spark.streamdm.streams._
import org.apache.spark.streamdm.evaluation.Evaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.streamdm.classifiers.trees.HoeffdingTree

import java.time.{Instant, ZoneId, ZonedDateTime}

/**
 * Task for evaluating a classifier on a stream by testing then training with
 * each example in sequence.
 *
 * <p>It uses the following options:
 * <ul>
 *  <li> Learner (<b>-c</b>), an object of type <tt>Classifier</tt>
 *  <li> Evaluator (<b>-e</b>), an object of type <tt>Evaluator</tt>
 *  <li> Reader (<b>-s</b>), a reader object of type <tt>StreamReader</tt>
 *  <li> Writer (<b>-w</b>), a writer object of type <tt>StreamWriter</tt>
 * </ul>
 */
class EvaluatePrequential extends Task with Logging {

  val learnerOption:ClassOption = new ClassOption("learner", 'l',
    "Learner to use", classOf[Classifier], "org.apache.spark.streamdm.classifiers.trees.HoeffdingTree")

  val evaluatorOption:ClassOption = new ClassOption("evaluator", 'e',
    "Evaluator to use", classOf[Evaluator], "BasicClassificationEvaluator")

  val streamReaderOption:ClassOption = new ClassOption("streamReader", 's',
    "Stream reader to use", classOf[StreamReader], "org.apache.spark.streamdm.streams.DirectoryReader")

  val resultsWriterOption:ClassOption = new ClassOption("resultsWriter", 'w',
    "Stream writer to use", classOf[StreamWriter], "PrintStreamWriter")

  val shouldPrintHeaderOption:FlagOption = new FlagOption("shouldPrintHeader", 'h',
    "Whether or not to print the evaluator header on the output file")

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Run the task.
   */
  def run(): Unit = {

    val reader: StreamReader = this.streamReaderOption.getValue()
    val schema: StructType = reader.getSchema()

    // TODO: Must change it to a generic classifier
    val learner: HoeffdingTree = this.learnerOption.getValue()
    learner.init(schema)

    val evaluator: Evaluator = this.evaluatorOption.getValue()
    evaluator.setSchema(schema)

    val writer: StreamWriter = this.resultsWriterOption.getValue()

    val stream: DataFrame = reader.getData()

//    TODO: How to specify not outputting the header now that the output is a table?
//    if(shouldPrintHeaderOption.isSet) {
//      writer.output(evaluator.header())
//    }
    // First, try to predict the class labels
    val streamWithPredictions = learner.predict(stream)

    val currentTimeUDF = udf { () =>
      Instant.ofEpochMilli(System.currentTimeMillis()).toString()
    }

    val streamWithProcessingTime = stream
      .withColumn("processingTime", to_timestamp(currentTimeUDF()))

    // Second, learn from the data
    learner.train(streamWithProcessingTime)

    // Third, create a stream of evaluation results (accuracy, recall, ...)
    val evaluationStream = evaluator.addResult(streamWithPredictions)



    //.withColumn("processingDate", current_timestamp())


//    import sparkSession.implicits._

//    val query = streamingWithProcessingDate
//
//      .groupBy(window($"processingTime", "20 seconds", "5 seconds"))
//        .count().select("*")
//
//      .writeStream
//      .outputMode("update")
//      .queryName("main-query")
//      .format("console")
//      .option("numRows", 100)
//      .start()
//
//    query.awaitTermination()


    //Predict
//    val predPairs = learner.predict(instances)

    //Train
//    learner.train(instances)

    //Evaluate
//    writer.output(evaluator.addResult(predPairs))

  }
}


