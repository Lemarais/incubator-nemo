/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.metric;

import org.apache.nemo.common.punctuation.Latencymark;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.Serializable;

/**
 * Metric class for delay.
 */
public class WatermarkMetric implements Serializable {
  private String sourceVertexId;
  private Watermark watermark;
  private long timestamp;

  /**
   * Constructor with the designated id, watermark timestamp and delay.
   *
   * @param watermark the id.
   * @param timestamp When an element was created.
   */
  public WatermarkMetric(final String sourceVertexId, final Watermark watermark, final long timestamp) {
    this.sourceVertexId = sourceVertexId;
    this.watermark = watermark;
    this.timestamp = timestamp;
  }

  public String getSourceVertexId() {
    return sourceVertexId;
  }

  /**
   * Get the recorded latency mark.
   *
   * @return latency mark.
   */
  public Watermark getWatermark() {
    return watermark;
  }

  /**
   * Get the timestamp when the latencymark is recorded.
   *
   * @return timestamp when it is recorded.
   */
  public long getTimestamp() {
    return this.timestamp;
  }
}
