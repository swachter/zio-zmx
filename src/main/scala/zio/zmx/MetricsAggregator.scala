package zio.zmx

import zio.UIO

object MetricsAggregator {

  sealed trait AddResult

  object AddResult {
    object Added   extends AddResult
    object Ignored extends AddResult
    object Dropped extends AddResult
  }

  trait Service {
    def add(m: Metric[_]): UIO[AddResult]
  }

}

object MetricsSender {

  trait Service[B] {
    def send(b: B): UIO[Unit]
  }

}
