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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter


class KinesisStreamWriter(streamName: String,
                          region: Regions,
                          chunk: Int,
                          endpoint: Option[String])
  extends StreamWriter with SupportsWriteInternalRow {

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def createInternalRowWriterFactory(): DataWriterFactory[InternalRow] =
    new KinesisStreamWriterFactory(streamName, region, chunk, endpoint)
}

class KinesisStreamWriterFactory(streamName: String,
                                 region: Regions,
                                 chunk: Int,
                                 endpoint: Option[String]) extends DataWriterFactory[InternalRow] {
  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] =
    new DataWriter[InternalRow] {

      private lazy val client = AmazonKinesis(streamName, region, chunk, endpoint = endpoint)

      def commit(): WriterCommitMessage = {
        client.flush()
        KinesisWriterCommitMessage
      }

      def abort(): Unit = {}

      def write(record: InternalRow): Unit = {
        client.putRecord(record)
      }
    }
}

case object KinesisWriterCommitMessage extends WriterCommitMessage
