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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

class DirectoryReader extends StreamReader {

  val schemaFileNameOption: StringOption = new StringOption("schemaFileName", 's',
    "Schema File Name", "../data/none.csv")

  val inputStreamPathOption: StringOption = new StringOption("inputStreamPath", 'f',
    "Input Stream Path", "../data/stream")

  val formatHeaderOption: StringOption = new StringOption("formatHeader", 'h',
    "Stream files format", "csv")

  val formatStreamOption: StringOption = new StringOption("formatStream", 'k',
    "Header file format", "csv")

  val classIdentifierOption: StringOption = new StringOption("classIdentifier", 'c',
    "The class attribute identifier in the schema file", "class")

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  var schema: StructType = null

  override def getData(): DataFrame = {
    val streamDF = sparkSession
      .readStream
      .format(this.formatStreamOption.getValue)
      .schema(this.getSchema())
      .option("maxFilesPerTrigger", 1)
      .load(this.inputStreamPathOption.getValue())

    // getSchema adds the class column
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

    import sparkSession.implicits._

    val formatType = this.formatHeaderOption.getValue
    val classIdentifier = this.classIdentifierOption.getValue

    if (this.schema == null) {
      formatType match {
        case "arff" =>
          val rawDF: DataFrame = sparkSession
            .read
            .option("header", false)
            .option("inferSchema", false)
            .text(this.schemaFileNameOption.getValue)

          val intermediaryDF = rawDF
              .filter(_.getString(0).toLowerCase().contains("@attribute"))
              .withColumn("identifier", split(col("value"), "\\s+").getItem(1))
              .withColumn("type", split(col("value"), "\\s+").getItem(2))
              .select("identifier", "type")

          val attributes = intermediaryDF.select("identifier", "type").as[(String, String)].collect()
          val schemaBuffer: ArrayBuffer[StructField] = ArrayBuffer.empty[StructField]

          for((attributeIdentifier, attributeType ) <- attributes) {

            // NOMINAL ATTRIBUTE... as it contains a list of values {...}
            if(attributeType.contains("{")) {
              val nominalValues: Array[String] =
                intermediaryDF.filter(row => row.getString(0) == attributeIdentifier).select("type")
                  .flatMap(m => m.getString(0).replaceAll("[{}]", "").split(",")).collect()

              val metadataBuilder = new MetadataBuilder()
                .putStringArray("values", nominalValues) // the nominal values in string form (original)
                .putString("type", "nominal")

              // Add each nominal value as a Long to create a map from string to index for the labels.
              for(index <- nominalValues.indices)
                metadataBuilder.putLong(nominalValues(index), index)

              // Special case for the class
              if(attributeIdentifier == classIdentifier) {
                metadataBuilder
                  .putLong("num_class", nominalValues.length) // save the number of class labels (could be inferred)
                  .putString("identifier", classIdentifier) // save the original class identifier

                // The attribute is identifed as "class" just to facilitate further manipulation
                schemaBuffer += StructField("class", IntegerType, true, metadataBuilder.build())
              }
              // Not the class label
              else {
                // Just for consistency with the attribute "class", the attribute main identifier matches this one.
                metadataBuilder.putString("identifier", attributeIdentifier)
                schemaBuffer += StructField(attributeIdentifier, IntegerType, true, metadataBuilder.build())
              }
            }
            // NUMERIC ATTRIBUTE (assume anything that is not nominal to be numerical, i.e. DoubleType)
            else {
              val metadataBuilder = new MetadataBuilder()
                .putString("type", "numeric")
                .putString("identifier", attributeIdentifier) // for consistency

              schemaBuffer += StructField(attributeIdentifier, DoubleType, true, metadataBuilder.build())
            }
          }

          this.schema = new StructType(schemaBuffer.toArray)

          // TODO: Test to access the class values from the metadata previously encoded in the class attribute
//          println("$$$ classValue = ")
//          val myClassValues = this.schema(this.schema.fieldIndex("class")).metadata.getStringArray("values")
//
//          for(cID <- myClassValues ) {
//            println(cID + " => " + this.schema(this.schema.fieldIndex("class")).metadata.getLong(cID))
//          }
//          println()
        case "csv" =>
          // This infers the Schema
          val rawDF: DataFrame = sparkSession
            .read
            .option("header", true)
            .option("inferSchema", true)
            .csv(this.schemaFileNameOption.getValue)

          // TODO: Assumes that the problem is binary. Best option would be read this from the input data (e.g. distinct on the class field)
          // class_identifier saves the original class identifier.
          val metadataClass = new MetadataBuilder()
            .putLong("num_class", 2)
            .putString("identifier", classIdentifier)
            .build()

          // This renames the original classIdentifier to "class"
          val newColumnClass = rawDF.col(classIdentifier).as("class", metadataClass)
          val streamDF = rawDF.withColumn("class", newColumnClass)
          this.schema = streamDF.schema
        case _ =>
      }
    }

    this.schema
  }

}
