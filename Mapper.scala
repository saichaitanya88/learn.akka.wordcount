package learn.akka.mapreduce
import akka.actor.Actor;
import akka.actor.ActorRef;

class Mapper(combinerRef: ActorRef) extends Actor{
  def receive = {
    case MapMessage(line) => {
      var words = line.replaceAll("[^A-Za-z0-9 ]", "").split(" ").filterNot { x => x == "" }
      var wordMap = if (line != "") words.map(_.toLowerCase()->1).toList else List[(String, Int)]()     
      sender().tell(MapResult(wordMap) , self)      
    }
  }
}