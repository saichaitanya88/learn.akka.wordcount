package learn.akka.mapreduce

sealed trait Message
case class StartJob(filename: String) extends Message

case class MapMessage(sentence: String) extends Message

case class CombineMessage(input: List[(String, Int)]) extends Message

case class ShuffleMessage(input: Map[String, Int]) extends Message
case object ShuffleResult extends Message

case class ReduceMessage(reduceInput: Map[String, List[Int]]) extends Message
case class ReduceResult(result: Map[String, Int]) extends Message
case object GetReduceResult extends Message

case class Complete() extends Message