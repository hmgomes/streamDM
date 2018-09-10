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
import java.time.Instant

import org.apache.spark.streamdm.preprocess.{StreamFilterRows, StreamPrepareForClassification}

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
class EvaluateTestThenTrain extends Task with Logging {

  val learnerOption: ClassOption = new ClassOption("learner", 'l',
    "Learner to use", classOf[Classifier], "org.apache.spark.streamdm.classifiers.trees.HoeffdingTree")

  val evaluatorOption: ClassOption = new ClassOption("evaluator", 'e',
    "Evaluator to use", classOf[Evaluator], "BasicClassificationEvaluator")

  val streamReaderOption: ClassOption = new ClassOption("streamReader", 's',
    "Stream reader to use", classOf[StreamReader], "org.apache.spark.streamdm.streams.DirectoryReader")

  // TODO: Adjust this option to set the format( <console, kafka, ...> ) from the query
  val resultsWriterOption: ClassOption = new ClassOption("resultsWriter", 'w',
    "Stream writer to use", classOf[StreamWriter], "PrintStreamWriter")

  val classAttributeOption: StringOption = new StringOption("classAttribute", 'c',
    "The class attribute identifier", "class")

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  def run(): Unit = {

    val reader: StreamReader = this.streamReaderOption.getValue()

    val streamRaw: DataFrame = reader.getData()

    // Vectors cannot represent null values and StreamPrepareForClassification executes a VectorAssembler, thus
    //  first it is necessary to get rid of rows that contain null values.
    // TODO: create an option to select the strategy to deal with null values.
    val preprocessedStream = new StreamFilterRows()
      .setDropNullRows(true)
      .transform(streamRaw)

    val stream = new StreamPrepareForClassification()
      .setClassAttributeIdentifier(this.classAttributeOption.getValue)
      .setNominalFeaturesMap(streamRaw)
      .transform(preprocessedStream)

    val learner: Classifier = this.learnerOption.getValue()
    learner.init(stream.schema)

    val evaluator: Evaluator = this.evaluatorOption.getValue()
    evaluator.setSchema(stream.schema)

    // *** PREDICTIONS ***: Obtain the predictions for each row
    val streamWithPredictions = learner.predict(stream)

    // Create a UDF to generate the process time column
    // TODO: current_timestamp() is not working on Spark 2.3.1, once it is working again this can be adjusted
    val currentTimeUDF = udf { () =>
      Instant.ofEpochMilli(System.currentTimeMillis()).toString()
    }

    // *** TRAIN ***: Train on the stream data
    val trainStream = learner.train(streamWithPredictions)

    // *** EVALUATE ***: Create a stream of evaluation results (accuracy, recall, ...)
    val evaluationStream = evaluator.addResult(streamWithPredictions)

    // Prepare the queries and start them
    val query = evaluationStream
      .withColumn("processingTime", to_timestamp(currentTimeUDF()))
      .writeStream
      .outputMode("update")
      .queryName("main-query")
      .format("console")
      .option("numRows", 100)
      .option("truncate", false)
      .start()

    val queryTrain = trainStream
      .withColumn("processingTime", to_timestamp(currentTimeUDF()))
      .writeStream
      .outputMode("update")
      .queryName("train-query")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
    queryTrain.awaitTermination()
  }
}
