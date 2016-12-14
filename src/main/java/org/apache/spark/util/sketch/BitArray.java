/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.sketch;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import me.lemire.integercompression.differential.*;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;

import java.util.*;

final class BitArray {
  private final long[] data;
  private long bitCount;

  static int numWords(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
    }
    long numWords = (long) Math.ceil(numBits / 64.0);
    if (numWords > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
    }
    return (int) numWords;
  }

  BitArray(long numBits) {
    this(new long[numWords(numBits)]);
  }

  private BitArray(long[] data) {
    this.data = data;
    long bitCount = 0;
    for (long word : data) {
      bitCount += Long.bitCount(word);
    }
    this.bitCount = bitCount;
  }

  /** Returns true if the bit changed value. */
  boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  /** Number of bits */
  long bitSize() {
    return (long) data.length * Long.SIZE;
  }

  /** Number of set bits (1s) */
  long cardinality() {
    return bitCount;
  }

  /** Combines the two BitArrays using bitwise OR. */
  void putAll(BitArray array) {
    assert data.length == array.data.length : "BitArrays must be of equal length when merging";
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= array.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  void writeTo0(DataOutputStream out) throws IOException {
    for(long chunk: data) {
      out.writeLong(chunk);
    }
  }

  void writeTo(DataOutputStream out) throws IOException {
    int[] sortedHashes = new int[(int)bitCount];
    int previousHash = 0;
    int idx = 0;
    for(int i=0;i<data.length;i++) {
      long chunk = data[i];
      int offset = 0;
      while(chunk != 0) {
        if((chunk & 1L) == 1L) {
          if(previousHash == 0) {
            sortedHashes[idx++] = i * 64 + offset;
            previousHash = i * 64 + offset;
          } else {
            int diff = (i * 64 + offset) - previousHash;
            sortedHashes[idx++] = diff;
            previousHash = i * 64 + offset; 
          }
        }
        chunk = chunk >>> 1L;
        offset += 1;
      }
    }
    double e = (data.length*64.0)/bitCount;
    System.out.printf("E=%1$f\n",e);
    int k = (int)Math.ceil(Math.log(e)/Math.log(2.0));
    System.out.printf("k=%1$d\n",k);
    int m = (int)Math.pow(2, k);
    System.out.printf("m=%1$d\n",m);
    java.util.BitSet vector = new java.util.BitSet();
    int pos = 0;
    int mask = 1 << k;
    for(int i=0;i<sortedHashes.length;i++) {
      int q = sortedHashes[i] >> k;
      while(q-->0) {
        vector.set(pos++,true);
      }
      vector.set(pos++,false);
      while(mask !=0) {
        if((sortedHashes[i] & mask) == 1) {
          vector.set(pos,true);
        } else {
          vector.set(pos, false);
        }
        mask = mask >> 1;
        pos++;
      }
    }
    byte[] bytes = vector.toByteArray();
    for(byte chunk: bytes) {
      out.writeByte(chunk);
    }

  }

  static BitArray readFrom(DataInputStream in) throws IOException {
    int numWords = in.readInt();
    long[] data = new long[numWords];
    for (int i = 0; i < numWords; i++) {
      data[i] = in.readLong();
    }
    return new BitArray(data);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || !(other instanceof BitArray)) return false;
    BitArray that = (BitArray) other;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
