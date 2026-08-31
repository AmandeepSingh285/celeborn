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
import java.util.concurrent.{Delayed, ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}

class ApplicationMetricsSourceSuite extends CelebornFunSuite {

  // Track every source created by a test so its metricsCleaner daemon thread can be shut down
  // in afterEach, instead of leaking a scheduled thread per source across the suite.
  private val createdSources = ArrayBuffer[ApplicationMetricsSource]()

  private def newSource(conf: CelebornConf): ApplicationMetricsSource = {
    val source = new ApplicationMetricsSource(conf)
    createdSources += source
    source
  }

  override def afterEach(): Unit = {
    createdSources.foreach(_.destroy())
    createdSources.clear()
    super.afterEach()
  }

  private def enabledConf(): CelebornConf = {
    val c = new CelebornConf()
    c.set(CelebornConf.MASTER_CLIENT_METRICS_ENABLED, true)
    c
  }

  private def scheduledTaskCount(source: ApplicationMetricsSource): Int =
    source.metricsCleaner.asInstanceOf[ScheduledThreadPoolExecutor].getQueue.size()

  private def gaugeMetrics(value: Long): JHashMap[String, ClientMetric] = {
    val map = new JHashMap[String, ClientMetric]()
    map.put("ClientActiveShuffleCount", ClientMetric(value, MetricType.Gauge))
    map
  }

  private def counterMetrics(value: Long, name: String = "ClientBytesWritten")
      : JHashMap[String, ClientMetric] = {
    val map = new JHashMap[String, ClientMetric]()
    map.put(name, ClientMetric(value, MetricType.Counter))
    map
  }

  // seq defaults to 0, which means "client does not sequence its reports" and is always
  // accepted, so tests that do not exercise the staleness guard behave as they did before it
  // existed.
  private def update(
      source: ApplicationMetricsSource,
      metrics: JHashMap[String, ClientMetric],
      labels: Map[String, String] = Map.empty,
      appId: String = "app-1",
      instanceId: String = "instance-1",
      seq: Long = 0L): Unit =
    source.updateApplicationMetrics(appId, labels, metrics, instanceId, seq)

  private def counterValue(
      source: ApplicationMetricsSource,
      labels: Map[String, String],
      name: String = "ClientBytesWritten"): Option[Long] =
    source.counters()
      .find(c => c.name == name && hasLabels(c.labels, labels))
      .map(_.counter.getCount)

  private def gaugeValue(
      source: ApplicationMetricsSource,
      labels: Map[String, String],
      name: String = "ClientActiveShuffleCount"): Option[Long] =
    source.gauges()
      .find(g => g.name == name && hasLabels(g.labels, labels))
      .map(_.gauge.getValue.asInstanceOf[Number].longValue())

  private def hasLabels(
      actual: Map[String, String],
      expected: Map[String, String]): Boolean =
    expected.forall { case (key, value) => actual.get(key).contains(value) }

  test("masterClientMetrics disabled: updateApplicationMetrics is a no-op") {
    val source = newSource(new CelebornConf())

    update(source, gaugeMetrics(5), Map("team" -> "data-eng"))

    assert(source.gauges().isEmpty)
    assert(source.counters().isEmpty)
  }

  test("masterClientMetrics disabled: removed app cleaner is not scheduled") {
    val source = newSource(new CelebornConf())

    assert(scheduledTaskCount(source) == 0)
  }

  test("removed app cleaner uses configured retention as schedule interval") {
    val conf = enabledConf()
    val retentionMs = 2000L
    conf.set(CelebornConf.MASTER_CLIENT_METRICS_REMOVED_APP_RETENTION, retentionMs)
    val source = newSource(conf)

    val scheduledTasks = source.metricsCleaner
      .asInstanceOf[ScheduledThreadPoolExecutor]
      .getQueue
    assert(scheduledTasks.size() == 1)
    val delayMs = scheduledTasks.peek().asInstanceOf[Delayed].getDelay(TimeUnit.MILLISECONDS)
    assert(delayMs > 0)
    assert(delayMs <= retentionMs)
  }

  test("no custom labels: metrics are not reported") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(3))

    assert(source.gauges().isEmpty)
    assert(source.counters().isEmpty)
  }

  test("client labels are used as metric labels") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("team" -> "data-eng"))

    val metrics = source.getMetrics
    assert(metrics.contains("""team="data-eng""""))
  }

  test("gauge is updated to the latest reported value") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(1), labels)
    update(source, gaugeMetrics(42), labels)

    assert(gaugeValue(source, labels).contains(42L))
  }

  test("gauge for a label set aggregates values across apps by sum") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    assert(gaugeValue(source, labels).contains(10L))
  }

  test("non-gauge metrics in heartbeat are silently ignored") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")
    val map = new JHashMap[String, ClientMetric]()
    map.put("ActiveShuffleCount", ClientMetric(3, MetricType.Gauge))

    source.updateApplicationMetrics("app-1", labels, map, "instance-1", 0L)

    assert(gaugeValue(source, labels, "ActiveShuffleCount").contains(3L))
    assert(source.counters().isEmpty)
  }

  test("removing one app updates gauge sum while another app still contributes") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    assert(gaugeValue(source, labels).contains(7L))
  }

  test("removing the last contributing app deregisters tracked metrics") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    source.removeApplicationMetrics("app-1")

    assert(gaugeValue(source, labels).contains(7L))

    source.removeApplicationMetrics("app-2")

    assert(gaugeValue(source, labels).isEmpty)
  }

  test("late heartbeats for removed apps are ignored") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    source.removeApplicationMetrics("app-1")
    update(source, gaugeMetrics(9), labels, "app-1")

    assert(gaugeValue(source, labels).isEmpty)
  }

  test("labels with invalid Prometheus key names are rejected") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("invalid-key" -> "ok"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("123start" -> "ok"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("valid_key" -> "ok"))
    assert(source.gauges().nonEmpty)
  }

  test("labels with unsafe values (quotes, backslashes, newlines) are rejected") {
    val source = newSource(enabledConf())

    update(source, gaugeMetrics(5), Map("team" -> """val"ue"""))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "val\\ue"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "val\nue"))
    assert(source.gauges().isEmpty)

    update(source, gaugeMetrics(5), Map("team" -> "safe_value"))
    assert(source.gauges().nonEmpty)
  }

  test("gauge values from multiple apps are summed, not last-writer-wins") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")

    assert(gaugeValue(source, labels).contains(10L))

    update(source, gaugeMetrics(5), labels, "app-1")
    assert(gaugeValue(source, labels).contains(12L))
  }

  test("counter values from multiple apps are summed") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1000), labels, "app-1")
    update(source, counterMetrics(500), labels, "app-2")

    assert(counterValue(source, labels).contains(1500L))
  }

  test("counters are published as Prometheus counters, not gauges") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1000), labels)

    // The Prometheus type is what tells downstream rate()/increase() how to read the series.
    val rendered = source.getMetrics
    assert(rendered.contains("# TYPE metrics_ClientBytesWritten_Count counter"))
    assert(!rendered.contains("metrics_ClientBytesWritten_Value"))
    assert(!source.gauges().exists(_.name == "ClientBytesWritten"))
  }

  // Scenario 3.1: the client's heartbeat timed out but the master had already applied it, so
  // the client retries. Values are absolute, so re-applying must be a no-op.
  test("redelivering the same report does not double count") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    (1 to 5).foreach(_ => update(source, counterMetrics(1500), labels, seq = 1L))

    assert(counterValue(source, labels).contains(1500L))
  }

  // Scenario 3.2: heartbeats are dropped entirely. The next one carries full absolute state,
  // so the master is correct again without any replay.
  test("dropped reports self-heal on the next report") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(100), labels, seq = 1L)
    // seq 2..9 never arrive
    update(source, counterMetrics(900), labels, seq = 10L)

    assert(counterValue(source, labels).contains(900L))
  }

  // Scenario 3.3: a delayed report overtaken by a newer one must not move a counter backwards.
  test("a stale report arriving after a newer one is dropped") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1800), labels, seq = 2L)
    update(source, counterMetrics(1500), labels, seq = 1L)

    assert(counterValue(source, labels).contains(1800L))
  }

  test("a report that does not advance the sequence is dropped") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1800), labels, seq = 2L)
    update(source, counterMetrics(9999), labels, seq = 2L)

    assert(counterValue(source, labels).contains(1800L))
  }

  test("the sequence guard is per application, not global") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(100), labels, "app-1", "instance-1", seq = 5L)
    // app-2's own sequence starts low; it must not be judged against app-1's.
    update(source, counterMetrics(200), labels, "app-2", "instance-2", seq = 1L)

    assert(counterValue(source, labels).contains(300L))
  }

  // Scenario 3.4: the client process restarts under the same appId, so its counter restarts
  // at zero. The previous process's total must be banked rather than subtracted.
  test("a client restart under the same appId does not decrease the counter") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1500), labels, "app-1", "instance-1", seq = 1L)
    assert(counterValue(source, labels).contains(1500L))

    // New process: counters restart at 0 and so does its sequence.
    update(source, counterMetrics(0), labels, "app-1", "instance-2", seq = 1L)
    assert(counterValue(source, labels).contains(1500L))

    update(source, counterMetrics(400), labels, "app-1", "instance-2", seq = 2L)
    assert(counterValue(source, labels).contains(1900L))
  }

  test("a gauge follows the new client instance instead of accumulating") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(7), labels, "app-1", "instance-1", seq = 1L)
    // A gauge is point-in-time: the restarted process's value replaces the old one.
    update(source, gaugeMetrics(2), labels, "app-1", "instance-2", seq = 1L)

    assert(gaugeValue(source, labels).contains(2L))
  }

  // Scenario 3.5: eviction must not make an exported counter non-monotonic, or every
  // downstream rate() reads the drop as a counter reset.
  test("removing an app retains its counter contribution") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1000), labels, "app-1")
    update(source, counterMetrics(500), labels, "app-2")
    assert(counterValue(source, labels).contains(1500L))

    source.removeApplicationMetrics("app-1")

    assert(counterValue(source, labels).contains(1500L))
  }

  test("a counter series survives removal of every reporting app") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1000), labels, "app-1")
    source.removeApplicationMetrics("app-1")

    // The series must stay published: restarting it from zero would look like a reset.
    assert(counterValue(source, labels).contains(1000L))
  }

  test("removing an app subtracts its gauge contribution") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    update(source, gaugeMetrics(7), labels, "app-2")
    assert(gaugeValue(source, labels).contains(10L))

    source.removeApplicationMetrics("app-1")

    // A departed app has no active shuffles, so it should stop contributing.
    assert(gaugeValue(source, labels).contains(7L))
  }

  test("a gauge series is unregistered once no app reports it") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, gaugeMetrics(3), labels, "app-1")
    source.removeApplicationMetrics("app-1")

    assert(source.gauges().isEmpty)
  }

  test("counters and gauges of the same name are tracked independently") {
    val source = newSource(enabledConf())
    val labels = Map("team" -> "data-eng")

    update(source, counterMetrics(1000, "SharedName"), labels)
    // A second report claiming a different type must not corrupt the existing series.
    update(source, gaugeMetrics(5), labels)

    assert(counterValue(source, labels, "SharedName").contains(1000L))
  }
}
