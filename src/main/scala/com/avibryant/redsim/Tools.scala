package com.avibryant.redsim.tools
import com.avibryant.redsim._
import scala.collection.mutable._

object Initialize extends App {
  val bands = args(0).toInt
  val rows = args(1).toInt
  val minCount = args(2).toInt
  val rs = new Redsim(new RedisConnection())

  rs.initialize(new Configuration(bands, rows, minCount))
}

object Load extends App {
  val filename = args(0)
  val lines = scala.io.Source.fromFile(filename).getLines
  var counter = 0

  lines.grouped(10000).
    foreach{group =>
      val buffer = new HashMap[String,ListBuffer[String]]()
      group.foreach{line =>
        val parts = line.split("\t")
        val set = parts(0)
        val item = parts(1)
        buffer.getOrElseUpdate(set, new ListBuffer[String]()) += (item)
        counter += 1
      }
      val rs = new Redsim(new RedisConnection())
      buffer.foreach{case (key, items) => rs.addItems(key, items.toList)}
      println(counter)
    }
}

object Dump extends App {
  val rs = new Redsim(new RedisConnection())
  rs.allSimilarCandidates.foreach {
    case (left, right) =>
    val sim = rs.similarity(left, right)
    val parts = List(sim.jaccard, sim.intersectionSize, left, right)
    println(parts.mkString("\t"))
  }
}