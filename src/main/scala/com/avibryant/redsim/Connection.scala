package com.avibryant.redsim

abstract class Hashing {
  def hashFor(str : String, seed : Int) : Int
  def bucketFor(hashes : Array[Int]) : Int
}

abstract class Connection {
  def readConfiguration : Configuration
  def writeConfiguration(config : Configuration) : Unit
  def readSignature(key : String)(implicit config : Configuration) : Option[Signature]
  def lockSignature(key : String)(fn : Option[Signature] => Unit)(implicit config : Configuration)
  def writeSignature(key : String, sig : Signature) : Unit
  def writeBuckets(key : String, sig : Signature) : Unit
  def updateSignature(key : String, oldSig : Signature, newSig : Signature) : Unit
  def updateBuckets(key : String, oldSig : Signature, newSig : Signature) : Unit
  def readBucket(band : Int, bucket : Int) : List[String]
  def bucketsWithCandidates(band : Int) : List[String]
  def reset : Unit
  implicit def hashing : Hashing
}