package ru.ponkin.test

import java.io.FileOutputStream
import java.util.zip.{ GZIPOutputStream, ZipOutputStream }
import com.esotericsoftware.kryo.io.Output
import org.apache.spark.util.sketch.BloomFilter
import com.esotericsoftware.kryo.Kryo

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
  val compressedFilter = BloomFilter.createCompressed(numItems)
  val items = Array.fill(numItems) { r.nextString(r.nextInt(512)) }

  time("old put") { items.foreach(oldFilter.put) }
  time("compressed put") { items.foreach(compressedFilter.put) }
  time("old mightContain") { items.foreach(oldFilter.mightContain) }
  time("compressed mightContain") { items.foreach(compressedFilter.mightContain) }

  val plain = new FileOutputStream("plain.bin")
  val compressed = new FileOutputStream("compressed.bin")
  oldFilter.writeTo(plain)
  compressedFilter.writeTo(compressed)
  plain.close()
  compressed.close()
}

