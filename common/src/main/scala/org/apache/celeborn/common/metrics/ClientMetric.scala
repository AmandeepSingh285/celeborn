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

package org.apache.celeborn.common.metrics

/**
 * What the master should do with an application's contribution to an aggregated
 * series when that application goes away (heartbeat timeout, or an explicit removal).
 *
 * This is the only behaviour that varies between metric types on the master side, so it
 * is declared on [[MetricType]] rather than branched on at each call site.
 */
sealed trait EvictionPolicy
object EvictionPolicy {

  /**
   * Drop the departed application's contribution. Correct for point-in-time values: a
   * gone application no longer has any active shuffles, so it should stop contributing
   * to the fleet total.
   */
  case object Subtract extends EvictionPolicy

  /**
   * Fold the departed application's last value into a tombstone accumulator that keeps
   * contributing to the exported series. Required for monotonic values: the events a
   * counter counted really did happen, and a decreasing counter makes every downstream
   * rate() misread the drop as a counter reset.
   */
  case object Retain extends EvictionPolicy
}

sealed trait MetricType {
  def evictionPolicy: EvictionPolicy
}
object MetricType {
  case object Gauge extends MetricType {
    override val evictionPolicy: EvictionPolicy = EvictionPolicy.Subtract
  }
  case object Counter extends MetricType {
    override val evictionPolicy: EvictionPolicy = EvictionPolicy.Retain
  }
}

/**
 * A single metric sample shipped from a client to the master on the application heartbeat.
 *
 * `value` is always absolute state, never a delta: for a counter it is the cumulative total
 * since the reporting client process started. The master's receive path is therefore a
 * replace rather than a read-modify-write, which is what makes redelivery of a heartbeat a
 * no-op and lets a dropped heartbeat self-heal on the next one. See
 * [[org.apache.celeborn.common.metrics.source.TrackedMetric]].
 */
case class ClientMetric(value: Long, metricType: MetricType)
