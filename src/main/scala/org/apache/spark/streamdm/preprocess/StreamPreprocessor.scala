package org.apache.spark.streamdm.preprocess

import org.apache.spark.sql.DataFrame

trait StreamPreprocessor extends Serializable {
  def transform(stream: DataFrame): DataFrame
}
