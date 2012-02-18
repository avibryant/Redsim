package com.avibryant.redsim

case class Configuration(
  val numRows : Int,
  val numBands : Int,
  val minCount : Int,
  val seeds : Array[Int]) {

  def this(nR : Int, nB : Int, mC : Int) = {
    this(nR, nB, mC, (1 to (nR * nB)).map{i => scala.util.Random.nextInt}.toArray)
  }

  def estimatedThreshold = math.pow(1.0/numBands, 1.0/numRows)
}

class Redsim(val conn : Connection) {
  implicit lazy val config = conn.readConfiguration
  implicit val hashing = conn.hashing

  def initialize(config : Configuration) {
    conn.reset
    conn.writeConfiguration(config)
  }

  def addItems(key : String, items : List[String]) {
    addSignature(key, items.map{new Signature(_)}.reduce{(a,b) => a+b})
  }

  def addSignature(key : String, sig : Signature) {
    conn.lockSignature(key) {
      case None => {
        conn.writeSignature(key, sig)
        if(sig.count >= config.minCount)
          conn.writeBuckets(key, sig)
      }
      case Some(oldSig) => {
        val newSig = sig + oldSig
        conn.updateSignature(key, oldSig, newSig)
        if(newSig.count >= config.minCount) {
          if(oldSig.count >= config.minCount)
            conn.updateBuckets(key, oldSig, newSig)
          else
            conn.writeBuckets(key, newSig)
        }
      }
    }
  }

  def allSimilarCandidates = {
    val bucketsOfKeys = (0 until config.numBands).flatMap{
      band => conn.bucketsWithCandidates(band).map{
        bucket => conn.readBucket(band, bucket.toInt)
      }
    }

    bucketsOfKeys.flatMap{
      bucket => bucket.zipWithIndex.flatMap {
        case (key1, index) =>
          bucket.slice(index + 1, bucket.size).map {
            key2 =>
              (key1, key2)
          }
      }
    }.toSet
  }

  def candidatesSimilarTo(key : String) = {
    conn.readSignature(key) match {
      case Some(sig) => sig.buckets.zipWithIndex.flatMap{
        case (bucket, band) => conn.readBucket(band, bucket)
      }.toSet - key
      case None => sys.error("Could not find " + key)
    }
  }

  def similarity(key1 : String, key2 : String) = {
    (conn.readSignature(key1), conn.readSignature(key2)) match {
      case (Some(sig1), Some(sig2)) => sig1.similarityWith(sig2)
      case _ => sys.error("Could not find keys")
    }
  }
}

