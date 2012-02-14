package com.avibryant.redsim

case class Similarity(val jaccard : Float, val leftCount : Int, val rightCount : Int) {
  def intersectionSize =  ((leftCount + rightCount) / (1 + jaccard)).toInt
  def unionSize = (leftCount + rightCount) - intersectionSize
  def cosine = unionSize.toFloat / (math.sqrt(leftCount) * math.sqrt(rightCount))
}

case class Signature(
    val count : Int,
    val values : Array[Int])(
    implicit val config : Configuration,
    implicit val hashing : Hashing) {

  def this(str : String)(implicit config : Configuration, hashing : Hashing) = {
    this(1, config.seeds.map{i => hashing.hashFor(str, i)})
  }

  def +(sig : Signature) = {
    val minValues = values.zip(sig.values).map{case (l,r) => l.min(r)}
    new Signature(count + sig.count, minValues)
  }

  def similarityWith(sig : Signature) = {
    val matching = values.size - updatedValues(sig).size
    val jaccard = matching.toFloat / values.size
    new Similarity(jaccard, count, sig.count)
  }

  def buckets = {
    values.grouped(config.numRows).map{a => hashing.bucketFor(a)}.toArray
  }

  def updatedBuckets(sig : Signature) = diffWithIndex(sig.buckets, buckets)
  def updatedValues(sig : Signature) = diffWithIndex(sig.values, values)

  private def diffWithIndex(before : Array[Int], after : Array[Int]) = {
    before.zip(after).zipWithIndex.filter{case ((b,a), i) => b != a}
  }
}
