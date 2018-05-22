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

package org.apache.bahir.sql

import scala.math.pow
import scala.util.Random

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry


package object kinesis {

  val recordsMaxCount = 500
  val recordMaxDataSize = 1024 * 1024
  val recordsMaxDataSize = 1024 * 1024 * 5

  case class PutRecordsEntry(entry: PutRecordsRequestEntry) {
    val recordSize = entry.getPartitionKey.getBytes.length + entry.getData.array.length
  }

  class AnalysisException(message: String) extends org.apache.spark.sql.AnalysisException(message)

  def sleepDuration(retry: Int, retryLimit: Int): Long = {
    // scaling factor
    val d = 0.5 + Random.nextDouble() * 0.1
    // possible seconds
    val durations = (0 until retryLimit).map(n => pow(2, n) * d)

    (durations(retry) * 1000).toLong
  }

}
