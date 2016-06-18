package learn.akka.mapreduce

import akka.actor.Actor;
import akka.actor.ActorRef;

class Combiner(shufflerRef: ActorRef, masterRef: ActorRef)  extends Actor{
  def receive = {
    case CombineMessage(input: List[(String,Int)]) => {
      val result = input.groupBy(f => f._1).map(m => (m._1, m._2.size));
      shufflerRef.tell(ShuffleMessage(result), self)
      masterRef.tell(ShuffleResult, self)
    }
  }
}