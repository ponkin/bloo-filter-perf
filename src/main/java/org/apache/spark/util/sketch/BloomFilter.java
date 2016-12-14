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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A Bloom filter is a space-efficient probabilistic data structure that offers an approximate
 * containment test with one-sided error: if it claims that an item is contained in it, this
 * might be in error, but if it claims that an item is <i>not</i> contained in it, then this is
 * definitely true. Currently supported data types include:
 * <ul>
 *   <li>{@link Byte}</li>
 *   <li>{@link Short}</li>
 *   <li>{@link Integer}</li>
 *   <li>{@link Long}</li>
 *   <li>{@link String}</li>
 * </ul>
 * The false positive probability ({@code FPP}) of a Bloom filter is defined as the probability that
 * {@linkplain #mightContain(Object)} will erroneously return {@code true} for an object that hasu
 * not actually been put in the {@code BloomFilter}.
 *
 * The implementation is largely based on the {@code BloomFilter} class from Guava.
 */
public abstract class BloomFilter {

  private static final int maxBuckets = 15;
  private static final int minBuckets = 2;
  private static final int minK = 1;
  private static final int maxK = 8;
  private static final int[] optKPerBuckets =
	  new int[]{1, // dummy K for 0 buckets per element
		    1, // dummy K for 1 buckets per element
		    1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14};

  /**
   * In the following table, the row 'i' shows false positive rates if i buckets
   * per element are used.  Column 'j' shows false positive rates if j hash
   * functions are used.  The first row is 'i=0', the first column is 'j=0'.
   * Each cell (i,j) the false positive rate determined by using i buckets per
   * element and j hash functions.
   */
  static final double[][] probs = new double[][]{
	  {1.0}, // dummy row representing 0 buckets per element
	  {1.0, 1.0}, // dummy row representing 1 buckets per element
	  {1.0, 0.393, 0.400},
	  {1.0, 0.283, 0.237, 0.253},
	  {1.0, 0.221, 0.155, 0.147, 0.160},
	  {1.0, 0.181, 0.109, 0.092, 0.092, 0.101}, // 5
	  {1.0, 0.154, 0.0804, 0.0609, 0.0561, 0.0578, 0.0638},
	  {1.0, 0.133, 0.0618, 0.0423, 0.0359, 0.0347, 0.0364},
	  {1.0, 0.118, 0.0489, 0.0306, 0.024, 0.0217, 0.0216, 0.0229},
	  {1.0, 0.105, 0.0397, 0.0228, 0.0166, 0.0141, 0.0133, 0.0135, 0.0145},
	  {1.0, 0.0952, 0.0329, 0.0174, 0.0118, 0.00943, 0.00844, 0.00819, 0.00846}, // 10
	  {1.0, 0.0869, 0.0276, 0.0136, 0.00864, 0.0065, 0.00552, 0.00513, 0.00509},
	  {1.0, 0.08, 0.0236, 0.0108, 0.00646, 0.00459, 0.00371, 0.00329, 0.00314},
	  {1.0, 0.074, 0.0203, 0.00875, 0.00492, 0.00332, 0.00255, 0.00217, 0.00199, 0.00194},
	  {1.0, 0.0689, 0.0177, 0.00718, 0.00381, 0.00244, 0.00179, 0.00146, 0.00129, 0.00121, 0.0012},
	  {1.0, 0.0645, 0.0156, 0.00596, 0.003, 0.00183, 0.00128, 0.001, 0.000852, 0.000775, 0.000744}, // 15
	  {1.0, 0.0606, 0.0138, 0.005, 0.00239, 0.00139, 0.000935, 0.000702, 0.000574, 0.000505, 0.00047, 0.000459},
	  {1.0, 0.0571, 0.0123, 0.00423, 0.00193, 0.00107, 0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
	  {1.0, 0.054, 0.0111, 0.00362, 0.00158, 0.000839, 0.000519, 0.00036, 0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
	  {1.0, 0.0513, 0.00998, 0.00312, 0.0013, 0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
	  {1.0, 0.0488, 0.00906, 0.0027, 0.00108, 0.00053, 0.000303, 0.000196, 0.00014, 0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
  };  // the first column is a dummy column representing K=0.

  public enum Version {
    /**
     * {@code BloomFilter} binary format version 1. All values written in big-endian order:
     * <ul>
     *   <li>Version number, always 1 (32 bit)</li>
     *   <li>Number of hash functions (32 bit)</li>
     *   <li>Total number of words of the underlying bit array (32 bit)</li>
     *   <li>The words/longs (numWords * 64 bit)</li>
     * </ul>
     */
    V1(1);

    private final int versionNumber;

    Version(int versionNumber) {
      this.versionNumber = versionNumber;
    }

    int getVersionNumber() {
      return versionNumber;
    }
  }

  /**
   * Returns the probability that {@linkplain #mightContain(Object)} erroneously return {@code true}
   * for an object that has not actually been put in the {@code BloomFilter}.
   *
   * Ideally, this number should be close to the {@code fpp} parameter passed in
   * {@linkplain #create(long, double)}, or smaller. If it is significantly higher, it is usually
   * the case that too many items (more than expected) have been put in the {@code BloomFilter},
   * degenerating it.
   */
  public abstract double expectedFpp();

  /**
   * Returns the number of bits in the underlying bit array.
   */
  public abstract long bitSize();

  /**
   * Puts an item into this {@code BloomFilter}. Ensures that subsequent invocations of
   * {@linkplain #mightContain(Object)} with the same item will always return {@code true}.
   *
   * @return true if the bloom filter's bits changed as a result of this operation. If the bits
   *     changed, this is <i>definitely</i> the first time {@code object} has been added to the
   *     filter. If the bits haven't changed, this <i>might</i> be the first time {@code object}
   *     has been added to the filter. Note that {@code put(t)} always returns the
   *     <i>opposite</i> result to what {@code mightContain(t)} would have returned at the time
   *     it is called.
   */
  public abstract boolean put(Object item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports {@code String} items.
   */
  public abstract boolean putString(String item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports {@code long} items.
   */
  public abstract boolean putLong(long item);

  /**
   * A specialized variant of {@link #put(Object)} that only supports byte array items.
   */
  public abstract boolean putBinary(byte[] item);

  /**
   * Determines whether a given bloom filter is compatible with this bloom filter. For two
   * bloom filters to be compatible, they must have the same bit size.
   *
   * @param other The bloom filter to check for compatibility.
   */
  public abstract boolean isCompatible(BloomFilter other);

  /**
   * Combines this bloom filter with another bloom filter by performing a bitwise OR of the
   * underlying data. The mutations happen to <b>this</b> instance. Callers must ensure the
   * bloom filters are appropriately sized to avoid saturating them.
   *
   * @param other The bloom filter to combine this bloom filter with. It is not mutated.
   * @throws IncompatibleMergeException if {@code isCompatible(other) == false}
   */
  public abstract BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException;

  /**
   * Returns {@code true} if the element <i>might</i> have been put in this Bloom filter,
   * {@code false} if this is <i>definitely</i> not the case.
   */
  public abstract boolean mightContain(Object item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests {@code String} items.
   */
  public abstract boolean mightContainString(String item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests {@code long} items.
   */
  public abstract boolean mightContainLong(long item);

  /**
   * A specialized variant of {@link #mightContain(Object)} that only tests byte array items.
   */
  public abstract boolean mightContainBinary(byte[] item);

  /**
   * Writes out this {@link BloomFilter} to an output stream in binary format. It is the caller's
   * responsibility to close the stream.
   */
  public abstract void writeTo(OutputStream out) throws IOException;

  /**
   * Reads in a {@link BloomFilter} from an input stream. It is the caller's responsibility to close
   * the stream.
   */
  public static BloomFilter readFrom(InputStream in) throws IOException {
    return OldBloomFilterImpl.readFrom(in);
  }

  /**
   * Computes the optimal k (number of hashes per item inserted in Bloom filter), given the
   * expected insertions and total number of bits in the Bloom filter.
   *
   * See http://en.wikipedia.org/wiki/File:Bloom_filter_fp_probability.svg for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param m total number of bits in Bloom filter (must be positive)
   */
  private static int optimalNumOfHashFunctions(long n, long m) {
    // (m / n) * log(2), but avoid truncation due to division!
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  /**
   * Computes m (total bits of Bloom filter) which is expected to achieve, for the specified
   * expected insertions, the required false positive probability.
   *
   * See http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives for the formula.
   *
   * @param n expected insertions (must be positive)
   * @param p false positive rate (must be 0 < p < 1)
   */
  private static long optimalNumOfBits(long n, double p) {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  static final double DEFAULT_FPP = 0.03;

  /**
   * Creates a {@link BloomFilter} with the expected number of insertions and a default expected
   * false positive probability of 3%.
   *
   * Note that overflowing a {@code BloomFilter} with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  public static BloomFilter create(long expectedNumItems) {
    return create(expectedNumItems, DEFAULT_FPP);
  }

  /**
   * Creates a {@link BloomFilter} with the expected number of insertions and expected false
   * positive probability.
   *
   * Note that overflowing a {@code BloomFilter} with significantly more elements than specified,
   * will result in its saturation, and a sharp deterioration of its false positive probability.
   */
  public static BloomFilter create(long expectedNumItems, double fpp) {
    if (fpp <= 0D || fpp >= 1D) {
      throw new IllegalArgumentException(
        "False positive probability must be within range (0.0, 1.0)"
      );
    }

    return create(expectedNumItems, optimalNumOfBits(expectedNumItems, fpp));
  }

  /**
   * Creates a {@link BloomFilter} with given {@code expectedNumItems} and {@code numBits}, it will
   * pick an optimal {@code numHashFunctions} which can minimize {@code fpp} for the bloom filter.
   */
  public static BloomFilter create(long expectedNumItems, long numBits) {
    if (expectedNumItems <= 0) {
      throw new IllegalArgumentException("Expected insertions must be positive");
    }

    if (numBits <= 0) {
      throw new IllegalArgumentException("Number of bits must be positive");
    }

    return new OldBloomFilterImpl(optimalNumOfHashFunctions(expectedNumItems, numBits), numBits);
  }

  /**
   * Create new bloom filters
   */

  public static BloomFilter createNew(long expectedNumItems) {
    return createNew(expectedNumItems, DEFAULT_FPP);
  }

  public static BloomFilter createNew(long expectedNumItems, double fpp) {
    if (fpp <= 0D || fpp >= 1D) {
      throw new IllegalArgumentException(
        "False positive probability must be within range (0.0, 1.0)"
      );
    }

    return createNew(expectedNumItems, optimalNumOfBits(expectedNumItems, fpp));
  }

  /**
   * Creates a {@link BloomFilter} with given {@code expectedNumItems} and {@code numBits}, it will
   * pick an optimal {@code numHashFunctions} which can minimize {@code fpp} for the bloom filter.
   */
  public static BloomFilter createNew(long expectedNumItems, long numBits) {
    if (expectedNumItems <= 0) {
      throw new IllegalArgumentException("Expected insertions must be positive");
    }

    if (numBits <= 0) {
      throw new IllegalArgumentException("Number of bits must be positive");
    }

    return new BloomFilterImpl(optimalNumOfHashFunctions(expectedNumItems, numBits), numBits);
  }

  public static BloomFilter create128(long expectedNumItems) {
    return create128(expectedNumItems, DEFAULT_FPP);
  }

  public static BloomFilter create128(long expectedNumItems, double fpp) {
    if (fpp <= 0D || fpp >= 1D) {
      throw new IllegalArgumentException(
        "False positive probability must be within range (0.0, 1.0)"
      );
    }

    return create128(expectedNumItems, optimalNumOfBits(expectedNumItems, fpp));
  }

  /**
   * Creates a {@link BloomFilter} with given {@code expectedNumItems} and {@code numBits}, it will
   * pick an optimal {@code numHashFunctions} which can minimize {@code fpp} for the bloom filter.
   */
  public static BloomFilter create128(long expectedNumItems, long numBits) {
    if (expectedNumItems <= 0) {
      throw new IllegalArgumentException("Expected insertions must be positive");
    }

    if (numBits <= 0) {
      throw new IllegalArgumentException("Number of bits must be positive");
    }

    return new OldBloomFilterImpl128(optimalNumOfHashFunctions(expectedNumItems, numBits), numBits);
  }

  public static BloomFilter createCompressed(long expectedNumItems) {
    if (DEFAULT_FPP >= probs[minBuckets][minK]) {
	    return new OldBloomFilterImpl(2, optKPerBuckets[2]*expectedNumItems);
    }
    if (DEFAULT_FPP < probs[maxBuckets][maxK]) {
	    return new OldBloomFilterImpl(maxK, maxBuckets*expectedNumItems);
    }

    // First find the minimal required number of buckets:
    int bucketsPerElement = 2;
    int K = optKPerBuckets[2];
    while (probs[bucketsPerElement][K] > DEFAULT_FPP) {
	    bucketsPerElement++;
	    K = optKPerBuckets[bucketsPerElement];
    }
    // Now that the number of buckets is sufficient, see if we can relax K
    // without losing too much precision.
    while (probs[bucketsPerElement][K - 1] <= DEFAULT_FPP) {
	    K--;
    }

    return new OldBloomFilterImpl(K, bucketsPerElement*expectedNumItems);
  }
}

