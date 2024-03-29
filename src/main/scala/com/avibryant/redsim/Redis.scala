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
    jedis.set(configKey.getBytes, intsToBytes(Array(
      config.numRows,
      config.numBands,
      config.minCount) ++
      config.seeds))
  }

  def reset {
     jedis.keys(prefix + "*").toArray(Array[String]()).foreach{
      jedis.del(_)
    }
  }

  def readSignature(key : String)(implicit config : Configuration) = {
    readInts(sigKey(key)) match {
      case(Array(count, values @ _*)) =>
        Some(new Signature(count.toInt, values.map{i => i.toInt}.toArray))
      case _ => None
    }
  }

  def lockSignature(key : String)(fn : Option[Signature] => Unit)(implicit config : Configuration) {
    jedis.watch(sigKey(key))
    val sig = readSignature(key)

    transaction = jedis.multi()
    fn(sig)
    val result = transaction.exec()
    transaction = null
    if(result == null) {
      System.err.println("Retrying " + key)
      lockSignature(key)(fn)
    }
  }

/*
  def writeSignature(key : String, sig : Signature) {
    checkInTransaction
    val sk = sigKey(key)
    transaction.rpush(sk, sig.count.toString)
    sig.values.foreach{i => transaction.rpush(sk, i.toString)}
    transaction.lpush(sigListKey, sk)
  }
*/
  def writeSignature(key : String, sig : Signature) {
    checkInTransaction
    transaction.set(sigKey(key).getBytes, intsToBytes(Array(sig.count) ++ sig.values))
    transaction.zadd(sigListKey, sig.count, key)
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
/*
  def updateSignature(key : String, oldSig : Signature, newSig : Signature) {
    checkInTransaction
    transaction.lset(sigKey(key), 0, newSig.count.toString)
    newSig.updatedValues(oldSig).foreach{
      case ((b,a), i) => transaction.lset(sigKey(key), i+1, a.toString)
    }
  }
*/

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
    stringList(jedis.zrangeByScore(bandKey(band), bucket, bucket))
  }

  def bucketsWithCandidates(band : Int) = {
    stringList(jedis.zrangeByScore(bandCountKey(band), 2, 2000))
  }

  def keysWithCandidates(implicit config : Configuration) = {
    stringList(jedis.zrangeByScore(sigListKey, config.minCount, 1000000))
  }

  private def stringList(set : java.util.Set[String]) = set.toArray(Array[String]()).toList

  implicit val hashing = new RedisHashing

  private def updateBucketCount(band : Int, bucket : Int, sign : Int) {
    transaction.zincrby(bandCountKey(band), sign.toDouble, bucket.toString)
  }

  private def configKey = prefix + "config"
  private def sigListKey = prefix + "sigList"
  private def sigKey(key : String) = prefix + "sig:" + key
  private def bandKey(band : Int) = prefix + "band:" + band
  private def bandCountKey(band : Int) = prefix + "bandCount:" + band
  private def prefix = "redsim:"
  private def checkInTransaction {
    if(transaction == null)
      sys.error("Not in a transaction")
  }

  private def readInts(key : String) : Array[Int] = {
    val bytes = jedis.get(key.getBytes)
    if(bytes == null)
      Array[Int]()
    else
      bytesToInts(bytes)
  }

  val messagePack = new org.msgpack.MessagePack

  private def bytesToInts(bytes : Array[Byte]) : Array[Int] = {
    val unpacker = messagePack.createBufferUnpacker
    unpacker.feed(bytes)
    var count = unpacker.readArrayBegin
    var ints : List[Int] = Nil
    while(count > 0) {
      ints = unpacker.readInt :: ints
      count -= 1
    }
    return ints.reverse.toArray
  }

  private def intsToBytes(ints : Array[Int]) : Array[Byte] = {
    val packer = messagePack.createBufferPacker
    packer.writeArrayBegin(ints.size)
    ints.foreach{i => packer.write(i)}
    packer.writeArrayEnd
    packer.toByteArray
  }
}