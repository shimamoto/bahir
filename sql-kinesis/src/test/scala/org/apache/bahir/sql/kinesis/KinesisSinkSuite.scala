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

import java.nio.ByteBuffer

import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest}


class KinesisSinkSuite extends StreamTest with MockitoSugar {
  import testImplicits._

  override def afterEach(): Unit = {
    AmazonKinesis.clear()
    super.afterEach()
  }

  test("batch - write to kinesis") {
    // stubbing
    val client = mock[com.amazonaws.services.kinesis.AmazonKinesis]
    when(client.putRecords(any())).thenReturn(
      new PutRecordsResult()
        .withFailedRecordCount(0)
        .withRecords(
          new PutRecordsResultEntry().withSequenceNumber("1").withShardId("shardId-000000000001"),
          new PutRecordsResultEntry().withSequenceNumber("2").withShardId("shardId-000000000001")
        )
    )
    AmazonKinesis(Regions.US_EAST_1, client)

    val df = batchDataset
    df.write
      .format("kinesis")
      .option("streamName", "test-stream")
      .option("region", "us-east-1")
      .save()

    verify(client, times(1)).putRecords(
      new PutRecordsRequest()
        .withStreamName("test-stream")
        .withRecords(
          new PutRecordsRequestEntry().withPartitionKey("partitionKey-1")
            .withData(ByteBuffer.wrap("data1".getBytes)),
          new PutRecordsRequestEntry().withPartitionKey("partitionKey-2")
            .withData(ByteBuffer.wrap("data2".getBytes))
        )
    )
  }

  test("batch - no stream name option") {
    val df = batchDataset
    val ex = intercept[IllegalArgumentException] {
      df.write
        .format("kinesis")
        .option("region", "us-east-1")
        .save()
    }
    assert(ex.getMessage.contains("'streamname' must be specified"))
  }

  test("batch - no region option") {
    val df = batchDataset
    val ex = intercept[IllegalArgumentException] {
      df.write
        .format("kinesis")
        .option("streamName", "test-stream")
        .save()
    }
    assert(ex.getMessage.contains("'region' must be specified"))
  }

  test("streaming - write to kinesis") {
    // stubbing
    val client = mock[com.amazonaws.services.kinesis.AmazonKinesis]
    when(client.putRecords(any())).thenReturn(
      new PutRecordsResult()
        .withFailedRecordCount(0)
        .withRecords(
          new PutRecordsResultEntry().withSequenceNumber("1").withShardId("shardId-000000000001")
        ),
      new PutRecordsResult()
        .withFailedRecordCount(0)
        .withRecords(
          new PutRecordsResultEntry().withSequenceNumber("2").withShardId("shardId-000000000001")
        )
    )
    AmazonKinesis(Regions.US_EAST_1, client)

    val input = MemoryStream[(String, String)]
    val query = streamingQuery(input.toDF().toDF("partitionKey", "data"))
    try {
      input.addData("partitionKey-1" -> "data1", "partitionKey-2" -> "data2")
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
    } finally {
      query.stop()
    }

    verify(client, times(1)).putRecords(
      new PutRecordsRequest()
        .withStreamName("test-stream")
        .withRecords(
          new PutRecordsRequestEntry().withPartitionKey("partitionKey-1")
            .withData(ByteBuffer.wrap("data1".getBytes))
        )
    )
    verify(client, times(1)).putRecords(
      new PutRecordsRequest()
        .withStreamName("test-stream")
        .withRecords(
          new PutRecordsRequestEntry().withPartitionKey("partitionKey-2")
            .withData(ByteBuffer.wrap("data2".getBytes))
        )
    )
  }

  test("streaming - write data with invalid schema") {
    val input = MemoryStream[String]
    val query = streamingQuery(input.toDF().toDF("partitionKey"))

    val ex = intercept[StreamingQueryException] {
      input.addData("partitionKey-1")
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
    }
    assert(ex.getMessage.contains("attribute 'data' not found"))
  }

  test("streaming - write data with wrong type key") {
    val input = MemoryStream[(Int, String)]
    val query = streamingQuery(input.toDF().toDF("partitionKey", "data"))

    val ex = intercept[StreamingQueryException] {
      input.addData(1 -> "data1")
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
    }
    assert(ex.getMessage.contains("partitionKey attribute type must be a String"))
  }

  test("streaming - write data with wrong type data") {
    val input = MemoryStream[(String, Int)]
    val query = streamingQuery(input.toDF().toDF("partitionKey", "data"))

    val ex = intercept[StreamingQueryException] {
      input.addData("partitionKey-1" -> 1)
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
    }
    assert(ex.getMessage.contains("data attribute type must be a String or BinaryType"))
  }


  private def batchDataset = {
    Seq("partitionKey-1" -> "data1", "partitionKey-2" -> "data2")
      .toDF("partitionKey", "data")
      .coalesce(1)
  }

  private def streamingQuery(df: DataFrame) = {
    val writer = df.writeStream
      .format("kinesis")
      .option("streamName", "test-stream")
      .option("region", "us-east-1")
    withTempDir { checkpointDir =>
      writer.option("checkpointLocation", checkpointDir.getCanonicalPath)
    }
    writer.start()
  }

}
