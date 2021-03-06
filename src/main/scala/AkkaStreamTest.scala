import java.io.File

import akka.actor.{ActorRef, Actor, Props}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, WatermarkRequestStrategy}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class AkkaStreamTest extends Actor {

  // actor system and implicit materializer
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  // read lines from a log file
  val logFile = new File("/home/nic/machado.txt")

  val stopWords = scala.io.Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  import akka.stream.io.Implicits._

  def getWords(ss: String) = {
    for {
      word <- ("""[.,\-\s]+""".r split ss).iterator
      lower = word.trim.toLowerCase
      if !(stopWords contains lower)
    } yield lower
  }

  val res = Source.synchronousFile(logFile)
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 8192, allowTruncation = true))
    .map(_.utf8String)
    .mapConcat(ss => getWords(ss).toVector)
    .to(Sink.actorSubscriber(Props(classOf[WordCountReducer], self))).run()

  res onSuccess { case _ => println("akkabou") }

  def receive = {
    case AkkaStreamTest.Hammerdown =>
      println("END THIS")
      context.system.terminate()
  }
}

object AkkaStreamTest {
  case object Hammerdown
}


class WordCountReducer(supervisor: ActorRef) extends ActorSubscriber {

  override val requestStrategy = new WatermarkRequestStrategy(6)

  var count = Map.empty[String, Long]

  def receive = {
    case OnNext(word: String) =>
      count = count + (word -> (1L + count.getOrElse(word, 0L)))

    case OnError(err: Exception) =>
      println("ERROR: " + err)
      context stop self

    case OnComplete =>
      println("FINISHED")
      PrintWordcountResults(count)
      context.system.scheduler.scheduleOnce(1 second, supervisor, AkkaStreamTest.Hammerdown)
      context stop self

    case xx => println("wtf:" + xx)
  }
}

object PrintWordcountResults {
  def apply[RedK, RedV](ag: Map[RedK, RedV])(implicit ev: Numeric[RedV]) = {
    import ev._
    println("FINAL RESULTS")
    ag.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:${i.toInt}%5d")
    }
  }
}
