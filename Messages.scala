package learn.akka.mapreduce

sealed trait Message
case class StartJob(filename: String) extends Message
case class MapMessage(sentence: String) extends Message
case class MapResult(results: List[(String, Int)]) extends Message
case class ReduceMessage(reduceInput: Map[String, List[Int]]) extends Message
case class ReduceResult(results: Map[String,Int]) extends Message
case class Complete() extends Message