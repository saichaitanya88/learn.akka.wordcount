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
    case MapResult(results: List[(String, Int)]) => {
      onMapResult(results)
    }
    case ReduceResult(results) => {
      onReduceResult(results)
    }
    case Complete() => {
      context.system.terminate()
    }
  }
  var mapRouter = {
      val routees = Vector.fill(nrOfMappers) {
        val r = context.actorOf(Props[Mapper])
        context watch r
        ActorRefRoutee(r)
      }
      Router(RoundRobinRoutingLogic(), routees)
    }
  var reduceRouter = {
    val routees = Vector.fill(nrOfReducers) {
      val r = context.actorOf(Props[Reducer])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }
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
	    mapRouter.route(new MapMessage(line), self)
    }
  }
  def onMapResult(results: List[(String, Int)]){
    mapResultCount += 1
    val percent = mapResultCount*100/linesCount
    if (percent > mapPercent){
      println(s"Map : $percent%")
      mapPercent = percent
    }
    results.foreach((s) => {
      if (!shuffleResult.containsKey(s._1)){
        shuffleResult.put(s._1, List(s._2))
      }
      else{
        shuffleResult.replace(s._1, s._2 :: shuffleResult.get(s._1))
      }
    })
    if (percent == 100){
      println("Map Finished in: " + (System.currentTimeMillis - start) + "ms")
      reduceRouter.route(new ReduceMessage(shuffleResult.asScala.toMap), self)
    }
  }
  def onReduceResult(results: scala.collection.Map[String,Int]){
    var total = 0; 
    results.foreach(f => total+=f._2)   
    println("Total Words: " + total)
    self.tell(Complete(), self)
  }
}