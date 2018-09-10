package org.apache.spark.streamdm.preprocess

import org.apache.spark.sql.DataFrame

class StreamFilterRows extends StreamPreprocessor {

  var dropNullRows: Boolean = false

  override def transform(stream: DataFrame): DataFrame = {
    if(dropNullRows)
      stream.na.drop
    else
      stream
  }

  def setDropNullRows(option: Boolean): StreamFilterRows = {
    dropNullRows = option
    this
  }
}
