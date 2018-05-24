/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bahir.sql.kinesis

import java.util.Locale

import com.amazonaws.regions.Regions

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * This provider is designed such that it throws [[IllegalArgumentException]]
 * when the Kinesis Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[kinesis] class KinesisSourceProvider extends DataSourceRegister
  with StreamSinkProvider
  with CreatableRelationProvider
  with StreamWriteSupport
  with Logging {
  import KinesisSourceProvider._

  def shortName(): String = "kinesis"

  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    val caseInsensitiveParams = validateOptions(parameters)

    val streamName = caseInsensitiveParams(STREAM_NAME_OPTION_KEY)
    val region = Regions.fromName(caseInsensitiveParams(REGION_OPTION_KEY))
    val chunk = caseInsensitiveParams.get(CHUNK_SIZE_OPTION_KEY).map(_.toInt)
      .getOrElse(defaultChunkSize)
    val endpoint = caseInsensitiveParams.get(ENDPOINT_OPTION_KEY)

    new KinesisSink(streamName, region, chunk, endpoint)
  }

  def createRelation(sqlContext: SQLContext,
                     mode: SaveMode,
                     parameters: Map[String, String],
                     data: DataFrame): BaseRelation = {
    val caseInsensitiveParams = validateOptions(parameters)

    val streamName = caseInsensitiveParams(STREAM_NAME_OPTION_KEY)
    val region = Regions.fromName(caseInsensitiveParams(REGION_OPTION_KEY))
    val chunk = caseInsensitiveParams.get(CHUNK_SIZE_OPTION_KEY).map(_.toInt)
      .getOrElse(defaultChunkSize)
    val endpoint = caseInsensitiveParams.get(ENDPOINT_OPTION_KEY)

    KinesisWriter.write(data.queryExecution, streamName, region, chunk, endpoint)

    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Kinesis write " +
          "operation is not usable.")
    }
  }

  def createStreamWriter(queryId: String,
                         schema: StructType,
                         mode: OutputMode,
                         options: DataSourceOptions): StreamWriter = {
    import scala.collection.JavaConverters._

    val caseInsensitiveParams = validateOptions(options.asMap.asScala.toMap)

    val streamName = caseInsensitiveParams(STREAM_NAME_OPTION_KEY)
    val region = Regions.fromName(caseInsensitiveParams(REGION_OPTION_KEY))
    val chunk = caseInsensitiveParams.get(CHUNK_SIZE_OPTION_KEY).map(_.toInt)
      .getOrElse(defaultChunkSize)
    val endpoint = caseInsensitiveParams.get(ENDPOINT_OPTION_KEY)

    KinesisWriter.validateQuery(schema)

    new KinesisStreamWriter(streamName, region, chunk, endpoint)
  }

  private def validateOptions(parameters: Map[String, String]): Map[String, String] = {
    val p = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    if (!p.contains(STREAM_NAME_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"Option '$STREAM_NAME_OPTION_KEY' must be specified for Kinesis Sink.")
    }
    if (!p.contains(REGION_OPTION_KEY)) {
      throw new IllegalArgumentException(
        s"Option '$REGION_OPTION_KEY' must be specified for Kinesis Sink.")
    }

    p
  }

}

object KinesisSourceProvider {
  val STREAM_NAME_OPTION_KEY = "streamname"
  val REGION_OPTION_KEY = "region"
  val CHUNK_SIZE_OPTION_KEY = "chunksize"
  val ENDPOINT_OPTION_KEY = "endpoint"


  private val defaultChunkSize = 50

}
