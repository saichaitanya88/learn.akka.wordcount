package learn.akka.mapreduce

import akka.actor.Actor;

class Combiner extends Actor{
  val reduceResult = 1;
  def receive = {
    case CombineMessage(input: Any) => {
      
    }
  }
}