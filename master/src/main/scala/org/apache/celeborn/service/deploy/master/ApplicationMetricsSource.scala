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

package org.apache.celeborn.service.deploy.master

import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}
import org.apache.celeborn.common.util.JavaUtils

class ApplicationMetricsSource(conf: CelebornConf)
  extends AbstractSource(conf, Role.MASTER) with Logging {
  override val sourceName = "application"

  private val masterClientMetricsEnabled = conf.masterClientMetricsEnabled

  private val gaugeValues =
    JavaUtils.newConcurrentHashMap[(Map[String, String], String), AtomicLong]()

  def updateApplicationMetrics(
      metricLabels: Map[String, String],
      metrics: JMap[String, ClientMetric]): Unit = {
    if (!masterClientMetricsEnabled || metricLabels.isEmpty) {
      return
    }

    metrics.asScala.foreach { case (name, metric) =>
      metric.metricType match {
        case MetricType.Gauge => updateGauge(metricLabels, name, metric.value)
        case MetricType.Counter => updateCounter(metricLabels, name, metric.value)
      }
    }
  }

  private def updateGauge(labels: Map[String, String], name: String, value: Long): Unit = {
    val ref = gaugeValues.computeIfAbsent(
      (labels, name),
      _ => {
        val r = new AtomicLong(0L)
        addGauge(name, labels) { () => r.get() }
        r
      })
    ref.set(value)
  }

  private def updateCounter(labels: Map[String, String], name: String, delta: Long): Unit = {
    if (delta <= 0) return
    if (!counterExists(name, labels)) {
      addCounter(name, labels)
    }
    incCounter(name, delta, labels)
  }
}
