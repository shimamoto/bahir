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

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_MAX_ERROR_RETRY
import com.amazonaws.services.kinesis.{AmazonKinesis => AWSKinesis, AmazonKinesisClientBuilder}
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsRequestEntry}
import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow


private[kinesis] object AmazonKinesis {

  private val cache = collection.concurrent.TrieMap.empty[Regions, AWSKinesis]

  def apply(
    streamName: String,
    region: Regions,
    chunk: Int,
    credentials: AWSCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance(),
    endpoint: Option[String] = None): AmazonKinesis = {

    val client = cache.getOrElseUpdate(region,
      endpoint.map { x =>
        AmazonKinesisClientBuilder.standard
          .withCredentials(credentials)
          .withEndpointConfiguration(new EndpointConfiguration(x, region.getName))
      }.getOrElse {
        AmazonKinesisClientBuilder.standard
          .withCredentials(credentials)
          .withRegion(region)
      }.build()
    )
    new AmazonKinesis(client, streamName, math.min(chunk, recordsMaxCount))
  }

  // This is only used for test
  def apply(region: Regions, client: AWSKinesis): AmazonKinesis = {
    new AmazonKinesis(cache.getOrElseUpdate(region, client), "dummy", recordsMaxCount)
  }
  // This is only used for test
  def clear(): Unit = {
    cache.clear()
  }

}

class AmazonKinesis(client: AWSKinesis, streamName: String, chunk: Int) extends Logging {

  private val buffer = new ArrayBuffer[PutRecordsEntry]()

  def putRecord(row: InternalRow): Unit = {
    val (partitionKey, data) = if (row.numFields == 1) {
      val payload = row.getBinary(0)
      DigestUtils.sha256Hex(payload) -> payload
    } else {
      row.getUTF8String(0).toString -> row.getBinary(1)
    }

    val entry = PutRecordsEntry(new PutRecordsRequestEntry()
      .withPartitionKey(partitionKey)
      .withData(ByteBuffer.wrap(data)))

    // record exceeds max size
    if (entry.recordSize > recordMaxDataSize) {
      logWarning(s"Record size limit exceeded in ${entry.recordSize/1024} KB, skipping...")
    } else {
      if (buffer.size >= chunk ||
        (buffer.map(_.recordSize).sum + entry.recordSize) >= recordsMaxDataSize) {
        flush()
      }

      buffer += entry
    }
  }

  def putRecords(data: Iterator[InternalRow]): Unit = {
    data foreach putRecord
  }

  def flush(): Unit = {
    @tailrec
    def put(records: Seq[PutRecordsEntry], retry: Int = 0): Unit = {
      val request = new PutRecordsRequest()
        .withStreamName(streamName)
        .withRecords(records.map(_.entry): _*)

      val failed = client.putRecords(request)
        .getRecords.asScala
        .zipWithIndex
        .collect { case (entry, i) if Option(entry.getErrorCode).nonEmpty => records(i) }

      // success
      if (failed.isEmpty) ()

      // exceed the upper limit of the retry
      else if (retry >= DEFAULT_MAX_ERROR_RETRY) throw new SparkException(
        s"Gave up after $retry retries")

      // retry
      else {
        val wait = sleepDuration(retry, DEFAULT_MAX_ERROR_RETRY)
        logWarning(s"Retrying a PutRecords request. Retry count: ${retry + 1}, " +
          s"Retry records: ${failed.size}, Wait millis $wait")
        Thread.sleep(wait)
        put(failed, retry + 1)
      }
    }

    put(buffer)
    buffer.clear()
  }

}
