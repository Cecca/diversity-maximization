package it.unipd.dei.diversity.akkaStream

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import com.codahale.metrics.MetricRegistry
import it.unipd.dei.diversity.source.PointSource
import it.unipd.dei.diversity.{Distance, Point, StreamingCoreset}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.reflect.ClassTag

object DiversitySink {

  val metrics = new MetricRegistry()

  def apply[T:ClassTag](kernelSize: Int, numDelegates: Int, distance: (T, T) => Double)
  : DiversitySink[T] =
    new DiversitySink[T](kernelSize, numDelegates, distance)

  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val k = 10
    val distance: (Point, Point) => Double = Distance.euclidean

    val source = Source.fromIterator(
      () => PointSource("random-gaussian-sphere", 256, 1000000, k, distance).iterator)

    val future = source.async.runWith(DiversitySink(100, k, distance))
    val result = Await.result(future, Duration.Inf)

    val processedPoints = metrics.meter("points")
    println(
      s"""
         |Processed ${processedPoints.getCount} points
         |Rate: ${processedPoints.getMeanRate} points/s
       """.stripMargin)
    println("Done!")
    system.terminate()
  }

}

class DiversitySink[T:ClassTag](val kernelSize: Int,
                                val numDelegates: Int,
                                val distance: (T, T) => Double)
  extends GraphStageWithMaterializedValue[SinkShape[T], Future[StreamingCoreset[T]]] {

  val in = Inlet[T]("DiversitySink.in")

  override def shape: SinkShape[T] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes)
  : (GraphStageLogic, Future[StreamingCoreset[T]]) = {

    val processedPoints = DiversitySink.metrics.meter("points")

    val coreset = new StreamingCoreset[T](kernelSize, numDelegates, distance)
    val promise = Promise[StreamingCoreset[T]]()

    val logic = new GraphStageLogic(shape) {
      override def preStart(): Unit = {
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          coreset.update(elem)
          processedPoints.mark()
          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          promise.success(coreset)
          super.onUpstreamFinish()
        }
      })
    }

    (logic, promise.future)
  }
}
