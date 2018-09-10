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

  val formatStreamOption: StringOption = new StringOption("formatStream", 'k',
    "Header file format", "csv")

//  val metaAttributeOption: StringOption = new StringOption("metaAttribute", 'c',
//    "The class attribute identifier in the schema file", "class")

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  var schema: StructType = null

  override def getData(): DataFrame = {

    this.formatStreamOption.getValue match {
      case "csv" =>
        val streamDF = sparkSession
          .readStream
          .format(this.formatStreamOption.getValue)
          .schema(this.getSchema())
          .option("maxFilesPerTrigger", 1)
          .load(this.inputStreamPathOption.getValue())

        streamDF
      case _ =>
        throw new Exception("The streaming data must be formatted as a CSV file, other formats are not supported yet.")
    }
  }

  /**
    * Obtains the Schema with feature specifications and other meta-data (e.g. num_values)
    *
    * @return an Schema of the features
    */
  override def getSchema(): StructType = {
    if (this.schema == null) {

      import sparkSession.implicits._

      val formatType = this.schemaFileNameOption.getValue.splitAt(this.schemaFileNameOption.getValue.lastIndexOf('.')+1)._2

      formatType match {
        case "header" =>
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
                .putLong("num_values", nominalValues.length)
                .putStringArray("values", nominalValues) // the nominal values in string form (original)
                .putString("type", "nominal")

              // Add each nominal value as a Long to create a map from string to index for the labels.
              for(index <- nominalValues.indices)
                metadataBuilder.putLong(nominalValues(index), index)

              // In case attributes are renamed latter, the original identifier is kept in "identifier"
              metadataBuilder.putString("identifier", attributeIdentifier)
              schemaBuffer += StructField(attributeIdentifier, StringType, true, metadataBuilder.build())
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

//          TODO: Reading from CSV is difficult since it is difficult to infer nominal features.
//        case "csv" =>
//          // This infers the Schema
//          val rawDF: DataFrame = sparkSession
//            .read
//            .option("header", true)
//            .option("inferSchema", true)
//            .csv(this.schemaFileNameOption.getValue)
//
//          // TODO: Assumes all other columns are numeric
//          for(att <- rawDF.columns) {
//
//          }
//
//          // TODO: Assumes that the problem is binary classification.
//          // Best option would be read this from the input data (e.g. distinct on the class field)
//          val metadataMeta = new MetadataBuilder()
//            .putString("type", "nominal")
//            .putLong("num_values", 2)
//            .putStringArray("values", Array("0", "1"))
//            .putString("identifier", metaAttributeIdentifier)
//
//
//          // This renames the original classIdentifier to "meta"
////          val newColumnClass = rawDF.col(metaAttributeIdentifier).as("meta", metadataClass)
////          val streamDF = rawDF.withColumn("meta", newColumnClass)
//          this.schema = streamDF.schema
        case _ =>
          throw new Exception("Unknown schema file header format = '" + formatType + "'")
      }
    }

    this.schema
  }

}
