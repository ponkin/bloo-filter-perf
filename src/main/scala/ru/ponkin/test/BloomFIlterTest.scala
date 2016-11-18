package ru.ponkin.test

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import org.apache.spark.util.sketch.BloomFilter

import scala.reflect.ClassTag
import scala.util.Random

object BloomFilterTest extends App {

  def time[R](label: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(s"$label - Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  val numItems = 1000000
  val r = new Random(32)

  val oldFilter = BloomFilter.create(numItems)
  val newFilter = BloomFilter.createNew(numItems)
  val old128Filter = BloomFilter.create128(numItems)
  val compressedFilter = BloomFilter.createCompressed(numItems)
  val items = Array.fill(numItems) { r.nextString(r.nextInt(512)) }

  time("new put") { items.foreach(newFilter.put) }
  time("old128 put") { items.foreach(old128Filter.put) }
  time("old put") { items.foreach(oldFilter.put) }
  time("compressed put") { items.foreach(compressedFilter.put) }
  time("new mightContain") { items.foreach(newFilter.mightContain) }
  time("old128 mightContain") { items.foreach(old128Filter.mightContain) }
  time("old mightContain") { items.foreach(oldFilter.mightContain) }
  time("compressed mightContain") { items.foreach(compressedFilter.mightContain) }

  val oldBos = new ByteArrayOutputStream()
  val newBos = new ByteArrayOutputStream()

  newFilter.writeTo(newBos)
  oldFilter.writeTo(oldBos)
  println(s"old size - ${oldBos.size()}")
  println(s"new size - ${newBos.size()}")

}

