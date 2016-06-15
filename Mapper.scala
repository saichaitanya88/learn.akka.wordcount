package learn.akka.mapreduce
import akka.actor.Actor;

class Mapper extends Actor{
  def receive = {
    case MapMessage(line) => {
      //println("Mapper.MapMessage");
      var words = line.replaceAll("[^A-Za-z0-9 ]", "").split(" ").filterNot { x => x == "" }
      var wordMap = if (line != "") words.map(_.toLowerCase()->1).toList else List[(String, Int)]()
      sender().tell(MapResult(wordMap) , self)
    }
  }
}