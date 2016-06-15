package learn.akka.mapreduce

import java.util.concurrent.CountDownLatch;
import akka.actor.ActorSystem;
import akka.actor.Props;
import scala.io.Source

object Application extends App {
  start()
  def start() = {
    val filename = "../Scrap/data/varney.txt"   
    val system = ActorSystem("AppSystem")
    val latch = new CountDownLatch(1);
    val master = system.actorOf(Props(new Master(10,1,latch)), name = "Master");
    master ! StartJob(filename)
    //latch.await();
    system.awaitTermination();
  }
}