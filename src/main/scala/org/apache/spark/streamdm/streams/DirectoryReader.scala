/*
 * Copyright (C) 2018 TelecomParis Tech.
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

package org.apache.spark.streamdm.streams

import com.github.javacliparser.StringOption
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DirectoryReader extends StreamReader {

  val schemaFileNameOption: StringOption = new StringOption("schemaFileName", 's',
    "Schema File Name", "../data/none.csv")

  val inputStreamPathOption: StringOption = new StringOption("inputStreamPath", 'f',
    "Input Stream Path", "../data/stream")

  val formatOption: StringOption = new StringOption("format", 'h',
    "File format", "csv")

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  var schema: StructType = null

  override def getData(): DataFrame = {
    val streamDF = sparkSession
      .readStream
      .format(this.formatOption.getValue)
      .schema(this.getSchema())
      .option("maxFilesPerTrigger", 1)
      .load(this.inputStreamPathOption.getValue())

    val inputColumns = streamDF.columns.filter(_ != "class")
    val assembler = new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("X")

    assembler.transform(streamDF)
  }

  /**
    * Obtains the Schema with feature specifications and other meta-data (e.g. num_class)
    *
    * @return an Schema of the features
    */
  override def getSchema(): StructType = {
    // TODO: Assumes the input file format is going to be CSV

    if (this.schema == null) {
      // This infers the Schema
      val rawDF: DataFrame = sparkSession
        .read
        .option("header", true)
        .option("inferSchema", true)
        .csv(this.schemaFileNameOption.getValue())

      // TODO: Assumes that the problem is binary. Best option would be read this from the input data.
      val metadataClass = new MetadataBuilder().putLong("num_class", 2).build()
      val newColumnClass = rawDF.col("class").as("class", metadataClass)
      val streamDF = rawDF.withColumn("class", newColumnClass)
      this.schema = streamDF.schema
    }

    this.schema
  }

}
