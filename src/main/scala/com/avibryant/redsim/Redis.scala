package com.avibryant.redsim
import redis.clients.jedis._
import redis.clients.util.MurmurHash

class RedisHashing extends Hashing {
  def hashFor(str : String, seed : Int) = MurmurHash.hash(str.getBytes, seed)
  def bucketFor(hashes : Array[Int]) = hashFor(hashes.mkString, hashes(0))
}

class RedisConnection(jedis : Jedis) extends Connection {
  def this() = this(new Jedis("localhost"))

  var transaction : Transaction = null

  def readConfiguration = {
    readInts(configKey) match {
      case(Array(numRows, numBands, minCount, seeds @ _*)) =>
        new Configuration(numRows, numBands, minCount, seeds.toArray)
      case _ => sys.error("Could not find configuration")
    }
  }

  def writeConfiguration(config : Configuration) {
    writeInts(configKey, Array(
      config.numRows,
      config.numBands,
      config.minCount) ++
      config.seeds)
  }

  def readSignature(key : String)(implicit config : Configuration) = {
    readInts(sigKey(key)) match {
      case(Array(count, values @ _*)) =>
        Some(new Signature(count, values.toArray))
      case _ => None
    }
  }

  def lockSignature(key : String)(fn : Option[Signature] => Unit)(implicit config : Configuration) {
    jedis.watch(key)
    val sig = readSignature(key)
    //not thread safe
    transaction = jedis.multi()
    fn(sig)
    transaction.exec()
    transaction = null
  }

  def writeSignature(key : String, sig : Signature) {
    checkInTransaction
    writeInts(transaction, sigKey(key), Array(sig.count) ++ sig.values)
  }

  def writeBuckets(key : String, sig : Signature) {
    checkInTransaction
    sig.buckets.zipWithIndex.foreach{
      case (bucket, band) => {
        transaction.zadd(bandKey(band), bucket, key)
        updateBucketCount(band, bucket, 1)
      }
    }
  }

  def updateSignature(key : String, oldSig : Signature, newSig : Signature) {
    writeSignature(key, newSig)
  }

  def updateBuckets(key : String, oldSig : Signature, newSig : Signature) {
    checkInTransaction
    newSig.updatedBuckets(oldSig).foreach{
      case ((b, a), i) => {
        transaction.zadd(bandKey(i), a, key)
        updateBucketCount(i, b, -1)
        updateBucketCount(i, a, 1)
      }
    }
  }

  def readBucket(band : Int, bucket : Int) = {
    jedis.zrangeByScore(bandKey(band), bucket, bucket).toArray(Array[String]()).toList
  }

  def bucketsWithCandidates(band : Int) = {
    jedis.zrangeByScore(bandCountKey(band), 2, 2000).toArray(Array[String]()).toList
  }

  implicit val hashing = new RedisHashing

  private def updateBucketCount(band : Int, bucket : Int, sign : Int) {
    transaction.zincrby(bandCountKey(band), sign.toDouble, bucket.toString)
  }

  private def configKey = prefix + "config"
  private def sigKey(key : String) = prefix + "sig:" + key
  private def bandKey(band : Int) = prefix + "band:" + band
  private def bandCountKey(band : Int) = prefix + "bandCount:" + band
  private def prefix = "redsim:"
  private def checkInTransaction {
    if(transaction == null)
      sys.error("Not in a transaction")
  }
  private def readInts(key : String) = {
    val str = jedis.get(key)
    if(str == null)
      Array[Int]()
    else
      str.split(":").map{_.toInt}
  }

  private def writeInts(txn : Transaction, key : String, ints : Array[Int]) {
    txn.set(key, ints.mkString(":"))
  }

  private def writeInts(key : String, ints : Array[Int]) {
    jedis.set(key, ints.mkString(":"))
  }

}