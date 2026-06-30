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

package org.apache.celeborn.common.metrics.source

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class AbstractSourcePerAppMetricsSuite extends CelebornFunSuite {

  private class TestSource extends AbstractSource(new CelebornConf(), Role.MASTER) {
    override val sourceName: String = "testSource"

    def testAddOrUpdateGaugeForApp(
        name: String,
        labels: Map[String, String],
        appId: String,
        value: Long): Unit =
      addOrUpdateGaugeForApp(name, labels, appId, value)

    def testAddOrUpdateCounterForApp(
        name: String,
        labels: Map[String, String],
        appId: String,
        delta: Long): Unit =
      addOrUpdateCounterForApp(name, labels, appId, delta)

    def testRemoveAppFromMetrics(appId: String): Unit =
      removeAppFromMetrics(appId)

    def testMetricNameWithCustomizedLabels(name: String, labels: Map[String, String]): String =
      metricNameWithCustomizedLabels(name, labels)

    def gaugeDetailsMap: java.util.concurrent.ConcurrentHashMap[String, NamedGaugeDetails[Long]] =
      namedGaugesWithDetails

    def counterDetailsMap: java.util.concurrent.ConcurrentHashMap[String, NamedCounterDetails] =
      namedCountersWithDetails
  }

  private def createSource(): TestSource = new TestSource()

  private val labels1 = Map("team" -> "data-eng")
  private val labels2 = Map("team" -> "infra")

  // --- addOrUpdateGaugeForApp ---

  test("addOrUpdateGaugeForApp registers gauge in namedGauges and namedGaugesWithDetails") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 42L)

    assert(source.gauges().exists(_.name == "TestGauge"))
    val metricKey = source.testMetricNameWithCustomizedLabels("TestGauge", labels1)
    val details = source.gaugeDetailsMap.get(metricKey)
    assert(details != null)
    assert(details.namedGauge.name == "TestGauge")
    assert(details.namedGauge.gauge.getValue == 42L)
    assert(details.appIds.contains("app-1"))
  }

  test("addOrUpdateGaugeForApp updates value on subsequent calls") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 99L)

    val metricKey = source.testMetricNameWithCustomizedLabels("TestGauge", labels1)
    assert(source.gaugeDetailsMap.get(metricKey).namedGauge.gauge.getValue == 99L)
    val gauge = source.gauges().find(_.name == "TestGauge")
    assert(gauge.get.gauge.getValue == 99L)
  }

  test("addOrUpdateGaugeForApp tracks multiple appIds for the same metric key") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-2", 20L)

    val metricKey = source.testMetricNameWithCustomizedLabels("TestGauge", labels1)
    val details = source.gaugeDetailsMap.get(metricKey)
    assert(details.appIds.size() == 2)
    assert(details.appIds.contains("app-1"))
    assert(details.appIds.contains("app-2"))
    assert(source.gauges().count(_.name == "TestGauge") == 1)
  }

  test("addOrUpdateGaugeForApp with different labels creates independent gauges") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("TestGauge", labels2, "app-2", 20L)

    assert(source.gauges().count(_.name == "TestGauge") == 2)
    assert(source.gaugeDetailsMap.size() == 2)
  }

  // --- addOrUpdateCounterForApp ---

  test("addOrUpdateCounterForApp registers counter in namedCounters and namedCountersWithDetails") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 5L)

    assert(source.counters().exists(_.name == "TestCounter"))
    val metricKey = source.testMetricNameWithCustomizedLabels("TestCounter", labels1)
    val details = source.counterDetailsMap.get(metricKey)
    assert(details != null)
    assert(details.namedCounter.name == "TestCounter")
    assert(details.appIds.contains("app-1"))
    assert(details.namedCounter.counter.getCount == 5L)
  }

  test("addOrUpdateCounterForApp ignores non-positive deltas") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 0L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", -5L)

    val counter = source.counters().find(_.name == "TestCounter")
    assert(counter.get.counter.getCount == 10L)
  }

  test("addOrUpdateCounterForApp does not register if only non-positive deltas provided") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 0L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", -1L)

    assert(source.counters().isEmpty)
    assert(source.counterDetailsMap.isEmpty)
  }

  test("addOrUpdateCounterForApp accumulates deltas and tracks multiple appIds") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-2", 25L)

    val metricKey = source.testMetricNameWithCustomizedLabels("TestCounter", labels1)
    val details = source.counterDetailsMap.get(metricKey)
    assert(details.appIds.size() == 2)
    assert(details.namedCounter.counter.getCount == 35L)
  }

  test("addOrUpdateCounterForApp with different labels creates independent counters") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels2, "app-2", 20L)

    assert(source.counters().count(_.name == "TestCounter") == 2)
    assert(source.counterDetailsMap.size() == 2)
  }

  // --- removeAppFromMetrics ---

  test("removeAppFromMetrics removes gauge when last app is removed") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 42L)
    source.testRemoveAppFromMetrics("app-1")

    assert(source.gauges().filter(_.name == "TestGauge").isEmpty)
    assert(source.gaugeDetailsMap.isEmpty)
  }

  test("removeAppFromMetrics preserves gauge when other apps remain") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-2", 20L)

    source.testRemoveAppFromMetrics("app-1")

    assert(source.gauges().exists(_.name == "TestGauge"))
    val metricKey = source.testMetricNameWithCustomizedLabels("TestGauge", labels1)
    val details = source.gaugeDetailsMap.get(metricKey)
    assert(details.appIds.size() == 1)
    assert(details.appIds.contains("app-2"))
  }

  test("removeAppFromMetrics removes counter when last app is removed") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testRemoveAppFromMetrics("app-1")

    assert(source.counters().filter(_.name == "TestCounter").isEmpty)
    assert(source.counterDetailsMap.isEmpty)
  }

  test("removeAppFromMetrics preserves counter when other apps remain") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-2", 25L)

    source.testRemoveAppFromMetrics("app-1")

    assert(source.counters().exists(_.name == "TestCounter"))
    val metricKey = source.testMetricNameWithCustomizedLabels("TestCounter", labels1)
    val details = source.counterDetailsMap.get(metricKey)
    assert(details.appIds.size() == 1)
    assert(details.appIds.contains("app-2"))
    assert(details.namedCounter.counter.getCount == 35L)
  }

  test("removeAppFromMetrics cleans metricRegistry") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 42L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)

    val gaugeKey = source.testMetricNameWithCustomizedLabels("TestGauge", labels1)
    val counterKey = source.testMetricNameWithCustomizedLabels("TestCounter", labels1)
    assert(source.metricRegistry.getMetrics.containsKey(gaugeKey))
    assert(source.metricRegistry.getMetrics.containsKey(counterKey))

    source.testRemoveAppFromMetrics("app-1")

    assert(!source.metricRegistry.getMetrics.containsKey(gaugeKey))
    assert(!source.metricRegistry.getMetrics.containsKey(counterKey))
  }

  test("re-registration after full removal cycle works for gauges") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 10L)
    source.testRemoveAppFromMetrics("app-1")

    assert(source.gauges().filter(_.name == "TestGauge").isEmpty)

    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-2", 99L)

    val gauge = source.gauges().find(_.name == "TestGauge")
    assert(gauge.isDefined)
    assert(gauge.get.gauge.getValue.asInstanceOf[Number].longValue() == 99L)
  }

  test("re-registration after full removal cycle works for counters") {
    val source = createSource()
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)
    source.testRemoveAppFromMetrics("app-1")

    assert(source.counters().filter(_.name == "TestCounter").isEmpty)

    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-2", 25L)

    val counter = source.counters().find(_.name == "TestCounter")
    assert(counter.isDefined)
    assert(counter.get.counter.getCount == 25L)
  }

  test("removeAppFromMetrics with unknown appId is a no-op") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("TestGauge", labels1, "app-1", 42L)
    source.testAddOrUpdateCounterForApp("TestCounter", labels1, "app-1", 10L)

    source.testRemoveAppFromMetrics("app-unknown")

    assert(source.gauges().exists(_.name == "TestGauge"))
    assert(source.counters().exists(_.name == "TestCounter"))
  }

  test("removeAppFromMetrics removes app from all metric keys it contributed to") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("Gauge1", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("Gauge2", labels2, "app-1", 20L)
    source.testAddOrUpdateCounterForApp("Counter1", labels1, "app-1", 5L)

    source.testRemoveAppFromMetrics("app-1")

    assert(source.gauges().isEmpty)
    assert(source.counters().isEmpty)
    assert(source.gaugeDetailsMap.isEmpty)
    assert(source.counterDetailsMap.isEmpty)
  }

  test("removeAppFromMetrics handles mixed: some metrics shared, some sole-owner") {
    val source = createSource()
    source.testAddOrUpdateGaugeForApp("SharedGauge", labels1, "app-1", 10L)
    source.testAddOrUpdateGaugeForApp("SharedGauge", labels1, "app-2", 20L)
    source.testAddOrUpdateGaugeForApp("SoleGauge", labels2, "app-1", 30L)

    source.testRemoveAppFromMetrics("app-1")

    assert(source.gauges().exists(_.name == "SharedGauge"))
    assert(!source.gauges().exists(_.name == "SoleGauge"))
    assert(source.gaugeDetailsMap.size() == 1)
  }
}
