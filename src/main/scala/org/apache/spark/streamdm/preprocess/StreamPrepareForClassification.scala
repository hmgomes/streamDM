package org.apache.spark.streamdm.preprocess

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.mutable.Map


class StreamPrepareForClassification extends StreamPreprocessor {

  var classAttributeIdentifier: String = _
  var nominalFeaturesMap: Map[String, Map[String, Long]] = Map.empty[String, Map[String, Long]]

  def setNominalFeaturesMap(df: DataFrame): StreamPrepareForClassification = {
    for(column <- df.columns) {
      var values: Map[String, Long] = Map.empty[String, Long]
      val featureType = df.schema(column).metadata.getString("type")
      if(featureType == "nominal") {
        for (value <- df.schema(column).metadata.getStringArray("values")) {
          values += (value -> df.schema(column).metadata.getLong(value))
        }
        nominalFeaturesMap += (column -> values)
      }
    }
    this
  }

  override def transform(stream: DataFrame): DataFrame = {
    assert(nominalFeaturesMap.nonEmpty)

    def nominalFeaturesMapUDF(feature: String) = udf((value: String) => nominalFeaturesMap(feature)(value))

    val nominalColumnsNames = nominalFeaturesMap.keys.toArray[String]
    val nominalColumns = nominalColumnsNames.map(column => (nominalFeaturesMapUDF(column)(col(column))))
    val nominalColumnsMetadata = nominalColumnsNames.map(column => stream.schema(column).metadata)

    val mappedStream = stream
      .withColumns(nominalColumnsNames, nominalColumns, nominalColumnsMetadata)

    // "X" does not include the classAttribute
    val inputColumns = mappedStream.columns.filter(_ != "class")
    val assembler = new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("X")

    // To facilitate manipulation by the Classifiers, we rename the original classAttribute to "class"
    val streamWithX = assembler.transform(mappedStream)
//    streamWithX.schema.foreach(field => println("field.name = " + field.name + " metadata = " + field.metadata ))

    streamWithX.withColumnRenamed(classAttributeIdentifier, "class")
  }

  def setClassAttributeIdentifier(classAttributeIdentifier: String) : StreamPrepareForClassification = {
    this.classAttributeIdentifier = classAttributeIdentifier
    this
  }
}
