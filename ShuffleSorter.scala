package learn.akka.mapreduce

import akka.actor.Actor;
import akka.actor.ActorRef;
import scala.collection.mutable.HashMap;



class ShuffleSorter extends Actor{
  val map = HashMap.empty[String,Int];
  def receive = {
    case ShuffleMessage(input: Map[String,Int]) => {
      println("ShuffleMessage");
      input.foreach(i => {
        map.put(i._1, map.getOrElse(i._1, 0) + i._2)
      });
    }
    case ReduceResult => {
      
    }
  }
}