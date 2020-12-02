package zio.zmx

import zio.clock.Clock
import zio.{ Chunk, UIO, ZIO, ZRef }
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.test.Assertion._
import zio.duration._
import zio.test.environment.TestClock
import zio.zmx.StmMetricsAggregator.BucketAggregationResult

object StmMetricsAggregatorSpec extends DefaultRunnableSpec {

  type MetricList = List[Metric[_]]

  def listAggregate(bucket: Option[MetricList], metric: Metric[_]): BucketAggregationResult[MetricList] =
    bucket match {
      case Some(list) if list.size == 4 => BucketAggregationResult.Next(List(metric))
      case Some(list)                   => BucketAggregationResult.Update(metric :: list)
      case None                         => BucketAggregationResult.Update(List(metric))
    }

  val aggregatorLayer =
    StmMetricsAggregator.layer(5, 1.second, StmMetricsAggregator.OverflowStrategy.Drop, listAggregate)

  def spec =
    suite("StmMetricsAggregatorSpec") {
      testM("does aggregate") {
        for {
          buckets <- ZRef.make(List.empty[MetricList])
          prog     = for {
                       aggregator <- ZIO.access[MetricsAggregator](_.get)
                       _          <- aggregator.add(Metric.Counter("counter", 1.0, 1.0, Chunk.empty))
                       _          <- ZIO.sleep(1500.milliseconds)
                     } yield ()
          sender   = new MetricsSender.Service[MetricList] {
                       override def send(b: MetricList): UIO[Unit] = ZIO.effectTotal(println("update")) *> buckets.update(b :: _)
                     }
          _       <- prog
                       .provideSomeLayer(aggregatorLayer)
                       .provideSome[TestClock with Clock](_.add(sender))
                       .fork
          _       <- ZIO.effect(println("adjust clock"))
          _       <- TestClock.adjust(2.seconds)
          _       <- ZIO.effect(println("adjusted clock"))
          bs      <- buckets.get
          _       <- ZIO.effect(println("got buckets"))
        } yield {
          println(s"assert size: ${bs.size}")
          assert { println("get size"); bs.size }(equalTo(1))
        }
      }
    }

}
