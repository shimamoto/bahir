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

import com.amazonaws.regions.Regions

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

/**
 * The [[KinesisWriter]] class is used to write data from a batch query
 * or structured streaming query, given by a [[QueryExecution]], to Kinesis.
 * The data is assumed to have a data column, and an optional partition key
 * column. If the partition key column is missing, then a SHA-256 digest of
 * data as a hex string will be added to the entry.
 */
private[kinesis] object KinesisWriter {
  val PARTITION_KEY_ATTRIBUTE_NAME = "partitionKey"
  val DATA_ATTRIBUTE_NAME = "data"

  def validateQuery(schema: StructType): Unit = {
    schema.fields
      .find(_.name == PARTITION_KEY_ATTRIBUTE_NAME)
      .filterNot(_.dataType == StringType)
      .foreach(_ => throw new AnalysisException(
        s"$PARTITION_KEY_ATTRIBUTE_NAME attribute type must be a String"))

    schema.fields
      .find(_.name == DATA_ATTRIBUTE_NAME)
      .orElse(throw new AnalysisException(s"Required attribute '$DATA_ATTRIBUTE_NAME' not found"))
      .filterNot(_.dataType == StringType)
      .filterNot(_.dataType == BinaryType)
      .foreach(_ => throw new AnalysisException(
        s"$DATA_ATTRIBUTE_NAME attribute type must be a String or BinaryType"))
  }

  def write(queryExecution: QueryExecution,
            streamName: String,
            region: Regions,
            chunk: Int,
            endpoint: Option[String] = None): Unit = {
    validateQuery(queryExecution.analyzed.schema)
    queryExecution.toRdd.foreachPartition { iter =>
      val client = AmazonKinesis(streamName, region, chunk, endpoint = endpoint)
      client.putRecords(iter)
      client.flush()
    }
  }

}
