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
      labels: Map[String, String] = Map.empty,
      appId: String = "app-1"): Unit =
    source.updateApplicationMetrics(appId, labels, metrics)

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

    source.updateApplicationMetrics("app-1", labels, map)

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 3L)
    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 10L)
  }

  test("removing sole app cleans up its gauges and counters") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    update(source, counterMetrics(10), labels, "app-1")

    assert(source.gauges().nonEmpty)
    assert(source.counters().nonEmpty)

    source.removeApplicationMetrics("app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)
    assert(source.counters().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("shared labels are not cleaned until last app is removed") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)

    source.removeApplicationMetrics("app-2")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("re-registration after removal works") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    source.removeApplicationMetrics("app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)

    update(source, gaugeMetrics(42), labels, "app-2")

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 42L)
  }

  test("removing unknown app is a no-op") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    source.removeApplicationMetrics("app-unknown")

    val gauge = source.gauges().find(_.labels.get("team").contains("data-eng"))
    assert(gauge.isDefined)
  }

  test("late heartbeat after app removal is ignored") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    source.removeApplicationMetrics("app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)

    update(source, gaugeMetrics(99), labels, "app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("removing sole app cleans up counters too") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels, "app-1")
    assert(source.counters().exists(_.labels.get("team").contains("data-eng")))

    source.removeApplicationMetrics("app-1")

    assert(source.counters().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("counter shared labels are not cleaned until last app is removed") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels, "app-1")
    update(source, counterMetrics(5), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 15L)

    source.removeApplicationMetrics("app-2")

    assert(source.counters().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("re-registration of counters after removal works") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(10), labels, "app-1")
    source.removeApplicationMetrics("app-1")

    assert(source.counters().filter(_.labels.get("team").contains("data-eng")).isEmpty)

    update(source, counterMetrics(25), labels, "app-2")

    val counter = source.counters().find(_.labels.get("team").contains("data-eng"))
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 25L)
  }

  test("different metric names with same labels are tracked independently") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")
    val map = new JHashMap[String, ClientMetric]()
    map.put("ShuffleCount", ClientMetric(3, MetricType.Gauge))
    map.put("WriteBytes", ClientMetric(100, MetricType.Gauge))

    source.updateApplicationMetrics("app-1", labels, map)

    val gauges = source.gauges().filter(_.labels.get("team").contains("data-eng"))
    assert(gauges.size == 2)

    source.removeApplicationMetrics("app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("multiple label sets for same app — removing app cleans all") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels1 = Map("team" -> "data-eng")
    val labels2 = Map("team" -> "infra")

    update(source, gaugeMetrics(5), labels1, "app-1")
    update(source, gaugeMetrics(10), labels2, "app-1")
    update(source, counterMetrics(15), labels1, "app-1")

    assert(source.gauges().size == 2)
    assert(source.counters().size == 1)

    source.removeApplicationMetrics("app-1")

    assert(source.gauges().filter(_.labels.get("team").contains("data-eng")).isEmpty)
    assert(source.gauges().filter(_.labels.get("team").contains("infra")).isEmpty)
    assert(source.counters().filter(_.labels.get("team").contains("data-eng")).isEmpty)
  }

  test("metricRegistry is cleaned after removal") {
    val source = new ApplicationMetricsSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(5), labels, "app-1")
    update(source, counterMetrics(10), labels, "app-1")

    assert(source.gaugeExists("ClientRegisterShuffleCount", labels))
    assert(source.counterExists("ClientRegisterShuffleCount", labels))

    source.removeApplicationMetrics("app-1")

    assert(!source.gaugeExists("ClientRegisterShuffleCount", labels))
    assert(!source.counterExists("ClientRegisterShuffleCount", labels))
  }
}
