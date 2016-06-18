package learn.akka.mapreduce

import java.util.concurrent.CountDownLatch
import akka.actor.Actor;
import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router, RoutingLogic }
import akka.actor.{Actor, PoisonPill, Props}
import akka.dispatch.Dispatchers
import scala.io.Source;
import scala.collection.concurrent.Map;
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.decorateAsScala._

class Master (nrOfMappers: Int, nrOfReducers: Int, latch: CountDownLatch) extends Actor{
  var start: Long = _
  var linesCount: Int = 0
  var mapResultCount: Int = 0
  var mapPercent: Int = 0
  var shuffleResult: ConcurrentHashMap[String, List[Int]] = new ConcurrentHashMap[String, List[Int]]()
  
  def receive = { 
    case StartJob(filename) => {
      onStart(filename)
    }
    case ShuffleResult => {
      mapResultCount += 1
      if (linesCount == mapResultCount){
        reducer.tell(GetReduceResult, self)
      }
    }
    case ReduceResult(map) => {
      println("ReduceResult");
      var total = 0
      map.foreach(m => total += m._2)
      println("uniquewords: " + map.size)
      println("total: " + total)
      self.tell(Complete(), self)
    }
    case Complete() => {
      context.system.terminate()
    }
  }
  
  val reducer = context.system.actorOf(Props[Reducer])
  var combiner = context.system.actorOf(Props(new Combiner(reducer, self)))
  var mapper = context.system.actorOf(Props(new Mapper(combiner)))
  
  override def preStart() {
    start = System.currentTimeMillis
  }
  
  override def postStop() {
    // tell the world that the calculation is complete
    println("Finished in: " + (System.currentTimeMillis - start) + "ms")
    latch.countDown()
  }
  def onStart(filename: String){
    println(s"Starting word count for '$filename'")
    val lines = Source.fromFile(filename).getLines
    for (line <- lines) {
      linesCount += 1
      mapper.tell(new MapMessage(line), self)
    }
  }
}