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

import java.util.{HashMap => JHashMap}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}

class ApplicationMetricsSourceSuite extends CelebornFunSuite {

  private def enabledConf(): CelebornConf = {
    val c = new CelebornConf()
    c.set(CelebornConf.MASTER_CLIENT_METRICS_ENABLED, true)
    c
  }

  private def gaugeMetrics(value: Long): JHashMap[String, ClientMetric] = {
    val map = new JHashMap[String, ClientMetric]()
    map.put("ClientRegisterShuffleCount", ClientMetric(value, MetricType.Gauge))
    map
  }

  private def counterMetrics(value: Long): JHashMap[String, ClientMetric] = {
    val map = new JHashMap[String, ClientMetric]()
    map.put("ClientRegisterShuffleCount", ClientMetric(value, MetricType.Counter))
    map
  }

  private def update(
      source: ApplicationMetricsSource,
      metrics: JHashMap[String, ClientMetric],
      labels: Map[String, String] = Map.empty): Unit =
    source.updateApplicationMetrics(labels, metrics)

  test("masterClientMetrics disabled: updateApplicationMetrics is a no-op") {
    val source = new ApplicationMetricsSource(new CelebornConf())

    update(source, gaugeMetrics(5), Map("team" -> "data-eng"))

    assert(source.gauges().isEmpty)
    assert(source.counters().isEmpty)
  }

  test("no custom labels: metrics are not reported") {
    val source = new ApplicationMetricsSource(enabledConf())

    update(source, gaugeMetrics(3))

    assert(source.gauges().isEmpty)
    assert(source.counters().isEmpty)
  }

  test("client labels are used as metric labels") {
    val source = new ApplicationMetricsSource(enabledConf())

    update(source, gaugeMetrics(5), Map("team" -> "data-eng"))

    val metrics = source.getMetrics
    assert(metrics.contains("""team="data-eng""""))
  }

  test("gauge is updated to the latest reported value") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(1), labels)
    update(source, gaugeMetrics(42), labels)

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 42L)
  }

  test("gauge for a label set is updated by whichever app heartbeats last") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels)
    update(source, gaugeMetrics(7), labels)

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 7L)
  }

  test("counter accumulates deltas from heartbeats") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels)
    update(source, counterMetrics(15), labels)

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 25L)
  }

  test("zero or negative counter delta is ignored") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels)
    update(source, counterMetrics(0), labels)
    update(source, counterMetrics(-5), labels)

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 10L)
  }

  test("counters from apps sharing a label set accumulate") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels)
    update(source, counterMetrics(5), labels)

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 15L)
  }

  test("counter deltas accumulate across sequential heartbeats") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels)
    update(source, counterMetrics(20), labels)
    update(source, counterMetrics(35), labels)

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 65L)
  }

  test("counter labels appear in prometheus output") {
    val source = new ApplicationMetricsSource(enabledConf())

    update(source, counterMetrics(10), Map("team" -> "infra"))

    val metrics = source.getMetrics
    assert(metrics.contains("""team="infra""""))
  }

  test("mixed gauge and counter in a single heartbeat") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")
    val map = new JHashMap[String, ClientMetric]()
    map.put("ActiveShuffleCount", ClientMetric(3, MetricType.Gauge))
    map.put("RegisterShuffleCount", ClientMetric(10, MetricType.Counter))

    source.updateApplicationMetrics(labels, map)

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 3L)
    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 10L)
  }
}
