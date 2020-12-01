package zio.zmx

import izumi.reflect.Tag
import zio.clock.Clock
import zio.duration.Duration
import zio.{ Has, UIO, URIO, URLayer, ZIO, ZLayer }
import zio.stm.{ TArray, TRef, USTM, ZSTM }
import zio.zmx.MetricsAggregator.AddResult

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

  def stmMetricsAggregator[B: Tag](
    size: Int,
    maxDelay: Duration,
    backpressureNotDrop: Boolean,
    aggregate: (Option[B], Metric[_]) => StmMetricsAggregator.BucketAggregationResult[B]
  ): URLayer[Clock with MetricsSender[B], MetricsAggregator] =
    ZLayer.fromEffect(StmMetricsAggregator(size, maxDelay, backpressureNotDrop, aggregate))

}

object MetricsSender {

  trait Service[B] {
    def send(b: B): UIO[Unit]
  }

}

object StmMetricsAggregator {

  sealed trait BucketAggregationResult[+B]

  object BucketAggregationResult {

    /** Indicates that a metric is ignored. */
    object IgnoreMetric extends BucketAggregationResult[Nothing]

    /**
     * Indicates that a bucket was updated with a metric.
     * The current bucket needs not to be shipped.
     */
    case class Update[B](b: B) extends BucketAggregationResult[B]

    /**
     * Indicates that aggregation must continue with the next bucket.
     * The current bucket must be shipped.
     */
    case class Next[B](next: B) extends BucketAggregationResult[B]
  }

  import BucketAggregationResult._

  private trait DoAddResult

  private object DoAddResult {
    object AddedToCurrentBucket            extends DoAddResult
    case class AddedToNextBucket(cnt: Int) extends DoAddResult
    object Ignored                         extends DoAddResult
    object Dropped                         extends DoAddResult
  }

  def apply[B: Tag](
    size: Int,
    maxDelay: Duration,
    backpressureNotDrop: Boolean,
    aggregate: (Option[B], Metric[_]) => BucketAggregationResult[B]
  ): URIO[Clock with MetricsSender[B], MetricsAggregator.Service] = for {
    clock             <- ZIO.access[Clock](_.get)
    sender            <- ZIO.access[MetricsSender[B]](_.get)
    // the index of the current bucket that is aggregated
    writeIdx          <- TRef.makeCommit(0)
    // the index of the next bucket to return
    readIdx           <- TRef.makeCommit(0)
    // counts the number of completed buckets
    // -> whenever writeIdx is incremented the counter is incremented, too.
    counter           <- TRef.makeCommit(0)
    buckets           <- TArray.fromIterable((0 until size).map(_ => Option.empty[B])).commit
    // fork a daemon that waits for completed buckets and forward them to the sender
    getCompletedBucket = (for {
                           rIdx <- readIdx.get
                           _    <- writeIdx.get.retryUntil(_ != rIdx)
                           b    <- buckets(rIdx)
                           _    <- readIdx.set((rIdx + 1) % size)
                         } yield b.get).commit
    _                 <- (getCompletedBucket >>= sender.send).forever.forkDaemon

  } yield new MetricsAggregator.Service {

    override def add(m: Metric[_]): UIO[AddResult] = for {
      addResult <- doAdd(m).commit
      res       <- addResult match {
                     case DoAddResult.AddedToNextBucket(startCnt) =>
                       // a new bucket was started
                       // -> fork a fiber that waits for the maxDelay and advances the writeIdx if the counter
                       //    did not change in the meantime
                       // -> this in turn completes the started bucket and causes it to be shipped
                       (for {
                         _ <- ZIO.sleep(maxDelay: Duration).provide(Has(clock))
                         _ <- (for {
                                currentCnt <- counter.get
                                _          <- if (startCnt == currentCnt) {
                                                for {
                                                  _ <- writeIdx.update(i => (i + 1) % size)
                                                  _ <- counter.modify(c => (c + 1, c + 1))
                                                } yield ()
                                              } else {
                                                ZSTM.succeed(())
                                              }
                              } yield ()).commit
                       } yield ()).forkDaemon *> ZIO.succeed(AddResult.Added)
                     case DoAddResult.AddedToCurrentBucket        => ZIO.succeed(AddResult.Added)
                     case DoAddResult.Ignored                     => ZIO.succeed(AddResult.Ignored)
                     case DoAddResult.Dropped                     => ZIO.succeed(AddResult.Dropped)
                   }
    } yield res

    /**
     * A transaction that determines what to do with the given metric and updates the buckets and writeIdx accordingly.
     */
    private def doAdd(metric: Metric[_]): USTM[DoAddResult] = for {
      wrtIdx    <- writeIdx.get
      oldBucket <- buckets(wrtIdx)
      addResult <- aggregate(oldBucket, metric) match {
                     case IgnoreMetric      =>
                       ZSTM.succeed(DoAddResult.Ignored)
                     case Update(newBucket) =>
                       for {
                         _         <- buckets.update(wrtIdx, _ => Some(newBucket))
                         addResult <- oldBucket match {
                                        case None    => counter.get.map(DoAddResult.AddedToNextBucket(_))
                                        case Some(_) => ZSTM.succeed(DoAddResult.AddedToCurrentBucket)
                                      }
                       } yield addResult
                     case Next(b)           =>
                       val nextWrtIdx = (wrtIdx + 1) % size
                       for {
                         drop      <- if (backpressureNotDrop) {
                                        // wait until the read index is not equal to the next write index
                                        readIdx.get.retryUntil(_ != nextWrtIdx).as(false)
                                      } else {
                                        // drop if the current read index would be equal to the next write index
                                        readIdx.get.map(_ == nextWrtIdx)
                                      }
                         addResult <- if (drop) {
                                        ZSTM.succeed(DoAddResult.Dropped)
                                      } else {
                                        for {
                                          _       <- buckets.update(nextWrtIdx, _ => Some(b))
                                          _       <- writeIdx.set(nextWrtIdx)
                                          started <- counter.modify(c => (c + 1, c + 1))
                                        } yield DoAddResult.AddedToNextBucket(started)
                                      }
                       } yield addResult
                   }
    } yield addResult

  }

}
