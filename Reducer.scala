package learn.akka.mapreduce

import akka.actor.Actor;

class Reducer extends Actor{
  val reduceResult = 1;
  def receive = {
    case ReduceMessage(reduceInput: Map[String, List[Int]]) => {
      sender().tell(ReduceResult(reduce(reduceInput)), self)
    }
  }
  
  def reduce(results: Map[String, List[Int]]) : Map[String, Int] = {
    results.map((f) => {
      f._1 -> f._2.size
    });
  }
}