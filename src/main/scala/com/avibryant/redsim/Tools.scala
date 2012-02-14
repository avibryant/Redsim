package com.avibryant.redsim.tools
import com.avibryant.redsim._

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
  val rs = new Redsim(new RedisConnection())
  var counter = 0

  lines.
    foreach{line =>
    val parts = line.split("\t")
    val set = parts(0)
    val item = parts(1)
    rs.addItems(set, List(item))
    counter += 1
    if(counter % 100 == 0)
      System.err.println(counter)
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