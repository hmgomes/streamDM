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

package org.apache.spark.streamdm.streams

import org.apache.spark.streamdm.core.Example
import org.apache.spark.streamdm.core.specification.ExampleSpecification
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.github.javacliparser.Configurable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Abstract class for outputting a DStream of Examples to be used
 * inside tasks.
 */
abstract class StreamReader extends Configurable {

  /**
    * Obtains a DataFrame stream of instances. Assumes a feature named "class" in the Schema, all other features
    * are assembled as part of "X".
    *
    * @return a DataFrame stream
    */
  def getData(): DataFrame

  /**
   * Obtains the specification of the examples in the stream.
   *
   * @return schema of the features
   */
  def getSchema(): StructType
}
