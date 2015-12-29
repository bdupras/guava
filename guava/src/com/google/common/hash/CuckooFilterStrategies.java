/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.hash;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.math.LongMath.mod;

/**
 * Collections of strategies of generating the f-bit fingerprint, index i1 and index i2 required for
 * an element to be mapped to a CuckooTable of m buckets with hash function h. These strategies are
 * part of the serialized form of the Cuckoo filters that use them, thus they must be preserved as
 * is (no updates allowed, only introduction of new versions).
 * <p/>
 * Important: the order of the constants cannot change, and they cannot be deleted - we depend
 * on their ordinal for CuckooFilter serialization.
 *
 * @author Brian Dupras
 * @author Alex Beal
 */
enum CuckooFilterStrategies implements CuckooFilter.Strategy {
    MURMUR128_BEALDUPRAS_32() {
        private static final int MAX_RELOCATION_ATTEMPTS = 500;
        private final HashFunction hashFunction = Hashing.murmur3_128();

        @Override
        public <T> boolean put(T object, Funnel<? super T> funnel, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, table.numBitsPerEntry);

            final long index = index(hash1, table.numBuckets);
            return putEntry(fingerprint, table, index) ||
                    putEntry(fingerprint, table, altIndex(index, fingerprint, table.numBuckets));
        }

        private boolean putEntry(int fingerprint, CuckooTable table, long index) {
            return table.swapAnyEntry(fingerprint, CuckooTable.EMPTY_ENTRY, index)
                    || putEntry(fingerprint, table, index, 0);
        }

        private boolean putEntry(final int fingerprint, final CuckooTable table, final long index, final int kick) {
            if (MAX_RELOCATION_ATTEMPTS == kick) {
                return false;
            }

            int entry = pickEntryToKick(table.numEntriesPerBucket);
            int kicked = table.swapEntry(fingerprint, index, entry);

            if ((CuckooTable.EMPTY_ENTRY == kicked) || putEntry(kicked, table, altIndex(index, kicked, table.numBuckets), kick + 1)) {
                return true;
            } else {
                int kickedBack = table.swapEntry(kicked, index, entry);
                assert kickedBack == fingerprint : "Uh oh - couldn't unroll failed attempts to putEntry()";
                return false;
            }
        }

        private final Random kicker = new Random(1L);

        private int pickEntryToKick(final int numEntriesPerBucket) {
            return kicker.nextInt(numEntriesPerBucket);
        }

        @Override
        public <T> boolean delete(T object, Funnel<? super T> funnel, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, table.numBitsPerEntry);
            final long index1 = index(hash1, table.numBuckets);
            final long index2 = altIndex(index1, fingerprint, table.numBuckets);
            return table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index1)
                    || table.swapAnyEntry(CuckooTable.EMPTY_ENTRY, fingerprint, index2);
        }

        @Override
        public <T> boolean mightContain(T object, Funnel<? super T> funnel, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, table.numBitsPerEntry);
            final long index1 = index(hash1, table.numBuckets);
            final long index2 = altIndex(index1, fingerprint, table.numBuckets);
            return table.hasEntry(fingerprint, index1) || table.hasEntry(fingerprint, index2);
        }

        @Override
        public boolean putAll(CuckooTable thiz, CuckooTable that) {
            for (long index = 0; index < that.numBuckets; index++) {
                for (int entry = 0; entry < that.numEntriesPerBucket; entry++) {
                    int fingerprint = that.readEntry(index, entry);
                    if (CuckooTable.EMPTY_ENTRY != fingerprint && !(
                            putEntry(fingerprint, thiz, index) ||
                                    putEntry(fingerprint, thiz,
                                            altIndex(index, fingerprint, thiz.numBuckets)))) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public boolean equivalent(CuckooTable thiz, CuckooTable that) {
            if (! thiz.isCompatible(that) ) { return false; }

            for (long index = 0; index < that.numBuckets; index++) {
                for (int entry = 0; entry < that.numEntriesPerBucket; entry++) {
                    int fingerprint = that.readEntry(index, entry);
                    if (CuckooTable.EMPTY_ENTRY == fingerprint) {
                        continue;
                    }

                    int thizCount = thiz.countEntry(fingerprint, index) + thiz.countEntry(fingerprint, altIndex(index, fingerprint, thiz.numBuckets));
                    int thatCount = that.countEntry(fingerprint, index) + that.countEntry(fingerprint, altIndex(index, fingerprint, that.numBuckets));
                    if (thizCount != thatCount) {
                        return false;
                    }
                }
            }
            return true;
        }

        <T> HashCode hash(final T object, final Funnel<? super T> funnel) {
            return hashFunction.hashObject(object, funnel);
        }

        int hash1(final long hash64) {
            return (int) hash64;
        }

        int hash2(final long hash64) {
            return (int) (hash64 >>> 32);
        }

        /**
         * Returns an f-bit portion of the given hash. Iterating by f-bit segments from the least
         * significant side of the hash to the most significant, looks for a non-zero segment. If a
         * non-zero segment isn't found, 1 is returned to distinguish the fingerprint from a
         * non-entry.
         *
         * @param hash 32-bit hash value
         * @param f number of bits to consider from the hash
         * @return first non-zero f-bit value from hash as an int, or 1 if no non-zero value is found
         */
        @Override
        public int fingerprint(final int hash, final int f) {
            checkArgument(f > 0, "f must be greater than zero");
            checkArgument(f <= Integer.SIZE, "f must be less than " + Integer.SIZE);
            int mask = (0x80000000 >> (f - 1)) >>> (Integer.SIZE - f);

            for (int bit = 0; (bit + f) <= Integer.SIZE; bit += f) {
                int ret = (hash >> bit) & mask;
                if (0 != ret) {
                    return ret;
                }
            }
            return 0x1;
        }

        /**
         * Calculates a primary index for an entry in the cuckoo table given the entry's 32-bit
         * hash and the table's size in buckets, m.
         *
         * tl;dr simply a wrap-around modulo bound by 0..m-1
         *
         * @param hash 32-bit hash value
         * @param m size of cuckoo table in buckets
         * @return index, bound by 0..m-1 inclusive
         */
        @Override
        public long index(final int hash, final long m) {
            return mod(hash, m);
        }

        /**
         * Calculates an alternate index for an entry in the cuckoo table.
         *
         * tl;dr
         * Calculates an offset as an odd hash of the fingerprint and adds to, or subtracts from,
         * the starting index, wrapping around the table (mod) as necessary.
         *
         * Detail:
         * Hash the fingerprint
         *   make it odd (*)
         *     flip the sign if starting index is odd
         *       sum with starting index (**)
         *         and modulo to 0..m-1
         *
         * (*) Constraining the CuckooTable to an even size in buckets, and applying odd offsets
         *     guarantees opposite parities for index & altIndex. The parity of the starting index
         *     determines whether the offset is subtracted from or added to the starting index.
         *     This strategy guarantees altIndex() is reversible, i.e.
         *
         *       index == altIndex(altIndex(index, fingerprint, m), fingerprint, m)
         *
         * (**) Summing the starting index and offset can possibly lead to numeric overflow. See
         *      {@link #protectedSum(long, long, long)} protectedSum} for details on how this is
         *      avoided.
         *
         * @param index starting index
         * @param fingerprint fingerprint
         * @param m size of table in buckets; must be even for this strategy
         * @return an alternate index for fingerprint bounded by 0..m-1
         */
        @Override
        public long altIndex(final long index, final int fingerprint, final long m) {
            checkArgument(0L <= index, "index must be a positive even number!");
            checkArgument((0L <= m) && (0L == (m & 0x1L)), "m must be a positive even number!");
            return mod(protectedSum(index, parsign(index) * odd(hash(fingerprint)), m), m);
        }

        /**
         * Maps parity of i to a sign.
         *
         * @return 1 if i is even parity, -1 if i is odd parity
         */
        long parsign(final long i) {
            return ((i & 0x01L) * -2L) + 1L;
        }

        int hash(final int i) {
            return hashFunction.hashInt(i).asInt();
        }

        long odd(final long i) {
            return i | 0x01L;
        }

        /**
         * Returns the sum of index and offset, reduced by a mod-consistent amount if necessary to
         * protect from numeric overflow. This method is intended to support a subsequent mod operation
         * on the return value.
         *
         * @param index Assumed to be >= 0L.
         * @param offset Any value.
         * @param mod Value used to reduce the result,
         * @return sum of index and offset, reduced by a mod-consistent amount if necessary to protect
         *         from numeric overflow.
         */
        long protectedSum(final long index, final long offset, final long mod) {
            return canSum(index, offset) ? index + offset : protectedSum(index - mod, offset, mod);
        }

        boolean canSum(final long a, final long b) {
            return (a ^ b) < 0 | (a ^ (a + b)) >= 0;
        }

    };

    public abstract int fingerprint(final int hash, final int f);

    public abstract long index(final int hash, final long m);

    public abstract long altIndex(final long index, final int fingerprint, final long m);

    static class CuckooTable {
        static final int EMPTY_ENTRY = 0x00;
        final long[] data;
        final long numBuckets;
        final int numEntriesPerBucket;
        final int numBitsPerEntry;
        private long size;
        private long checksum;

        CuckooTable(final long numBuckets, final int numEntriesPerBucket, final int numBitsPerEntry) {
            this(new long[calculateDataLength(numBuckets, numEntriesPerBucket, numBitsPerEntry)]
                    , numBuckets
                    , numEntriesPerBucket
                    , numBitsPerEntry
                    , 0L
            );
        }

        CuckooTable(final long[] data, final long numBuckets, final int numEntriesPerBucket, final int numBitsPerEntry, final long checksum) {
            this(data, 0L, checksum, numBuckets, numEntriesPerBucket, numBitsPerEntry);
        }

        CuckooTable(final long[] data, final long size, final long checksum, final long numBuckets, final int numEntriesPerBucket, final int numBitsPerEntry) {
            this.data = data;
            this.size = size;
            this.numBuckets = numBuckets;
            this.numEntriesPerBucket = numEntriesPerBucket;
            this.numBitsPerEntry = numBitsPerEntry;
            this.checksum = checksum;
        }

        CuckooTable copy() {
            return new CuckooTable(data.clone(), size, checksum, numBuckets, numEntriesPerBucket, numBitsPerEntry);
        }

        static int calculateDataLength(long numBuckets, int numEntriesPerBucket, int numBitsPerEntry) {
            checkArgument(numBuckets > 0, "numBuckets (%s) must be > 0", numBuckets);
            checkArgument(numEntriesPerBucket > 0, "numEntriesPerBucket (%s) must be > 0", numEntriesPerBucket);
            checkArgument(numBitsPerEntry > 0, "numBitsPerEntry (%s) must be > 0", numBitsPerEntry);

            return Ints.checkedCast(LongMath.divide(
                    LongMath.checkedMultiply(numBuckets,
                            LongMath.checkedMultiply(numEntriesPerBucket, numBitsPerEntry)),
                    Long.SIZE, RoundingMode.CEILING));
        }

        public int findEntry(final int value, final long bucket) {
            for (int i = 0; i < numEntriesPerBucket; i++) {
                if (value == readEntry(bucket, i)) {
                    return i;
                }
            }
            return -1;
        }

        public int countEntry(final int value, final long bucket) {
            int ret = 0;
            for (int i = 0; i < numEntriesPerBucket; i++) {
                if (value == readEntry(bucket, i)) {
                    ret ++;
                }
            }
            return ret;
        }

        public boolean hasEntry(final int value, final long bucket) {
            return findEntry(value, bucket) >= 0;
        }

        public int readEntry(final long bucket, final int entry) {
            return readBits(data, bitOffset(bucket, entry, numEntriesPerBucket, numBitsPerEntry), numBitsPerEntry);
        }

        public boolean swapAnyEntry(final int valueIn, final int valueOut, final long bucket) {
            final int entry = findEntry(valueOut, bucket);
            if (entry >= 0) {
                final int kicked = swapEntry(valueIn, bucket, entry);
                assert valueOut == kicked : "expected valueOut [" + valueOut + "] != actual kicked [" + kicked + "]";
                return true;
            }
            return false;
        }

        int swapEntry(final int value, final long bucket, final int entry) {
            final int kicked = writeBits(value, data, bitOffset(bucket, entry, numEntriesPerBucket, numBitsPerEntry), numBitsPerEntry);
            checksum += value - kicked;

            if ((EMPTY_ENTRY == value) && (EMPTY_ENTRY != kicked)) {
                size--;
            } else if ((EMPTY_ENTRY != value) && (EMPTY_ENTRY == kicked)) {
                size++;
            }
            assert size >= 0 : "Hmm - that's strange. CuckooTable size [" + size + "] shouldn't be < 0l";

            return kicked;
        }

        static long bitOffset(final long bucket, final int entry, final int numEntriesPerBucket, final int numBitsPerEntry) {
            return (bucket * numEntriesPerBucket + entry) * numBitsPerEntry;
        }

        static int dataIndex(final long bit) {
            return (int) (bit >>> 6);
        }

        @VisibleForTesting
        static int readBits(final long[] data, final long bit, final int len) {
            final int startLower = (int) (bit % Long.SIZE);
            final int lenLower = Math.min(Long.SIZE - startLower, len);
            final int lenUpper = Math.max(len - lenLower, 0);

            final int indexUpper = dataIndex(bit + len);

            final long lower = (data[dataIndex(bit)] & mask(startLower, lenLower)) >>> startLower;
            final long upper = indexUpper < data.length ? (data[indexUpper] & mask(0, lenUpper)) << lenLower : 0x00L;

            return (int) (lower | upper);
        }

        @VisibleForTesting
        static int writeBits(final int bits, final long[] data, final long bit, final int len) {
            final int ret = readBits(data, bit, len);

            final long bitsl = ((long) bits) & 0x00000000FFFFFFFFL; // upcast without carrying the sign

            final int startLower = (int) (bit % Long.SIZE);
            final int lenLower = Math.min(Long.SIZE - startLower, len);
            final int lenUpper = Math.max(len - lenLower, 0);

            final long maskLowerKeep = ~(mask(0, lenLower) << startLower);
            final long maskUpperKeep = mask(lenUpper, Long.SIZE - lenUpper);

            final long bitsLower = (bitsl << startLower) & ~maskLowerKeep;
            final long bitsUpper = (bitsl >>> (len - lenUpper)) & ~maskUpperKeep;

            final int indexLower = dataIndex(bit);
            final int indexUpper = dataIndex(bit + len - 1);

            final long dataLower = (data[indexLower] & maskLowerKeep) | bitsLower;
            data[indexLower] = dataLower;

            if (indexLower != indexUpper) {
                final long dataUpper = (data[indexUpper] & maskUpperKeep) | bitsUpper;
                data[indexUpper] = dataUpper;
            }

            return ret;
        }

        static long mask(final int start, final int len) {
            return (len <= 0) ? 0L : (0x8000000000000000L >> (len - 1)) >>> (Long.SIZE - (start + len));
        }

/*
             63     56      16      8       0
             -------+--...--+-------+-------|

        [22] -------+--...--+-------+-----010
             -------+--...--+-------+-----111

        [21] 10101--+--...--+-------+-------| ab
             11111--+--...--+-------+-------|

        [20] -------+--...--+---10101010----| xx
             -------+--...--+---11111111----|

        long[] data = new long[23]
        data[22] = 0x02
        data[21] = 0xA800000000000000L
        data[20] = 0x3550

        xx:
          startBit = 1285
          index1 = (int) (startBit >>> 6);
          len = 8
          padRight1 = (int)(startBit % Long.SIZE) = 5
          padLeft1 = Long.SIZE - (padRight1 + len) = 51
          mask1 = ((1L << (Long.SIZE-1)) >> (len-1)) >>> padLeft1 = Bx0001_1111_1110_0000 = 0x1FE0L = 8160
          ret = (int)((data[index1] & mask1) >> padRight1) = Bx1010_1010 = 0xAA = 170

       ab:
          startBit = 1403

          index1 = (int) (startBit >>> 6)
          index2 = (int) ((startBit + len) >>> 6)
          len = 8
          startBit1 = (int)(startBit % Long.SIZE)
          len1 = min(Long.SIZE - startBit1, len)
          len2 = max(len - len1, 0)

          lower = (data[index1] & maskLong(startBit1, len1)) >>> startBit1
          upper = (data[index2] & maskLong(0, len2)) << len1

          ret = lower | upper
*/

        @Override
        public boolean equals(Object o) {
            if (o instanceof CuckooTable) {
                CuckooTable that = (CuckooTable) o;
                return this.numBuckets == that.numBuckets
                        && this.numEntriesPerBucket == that.numEntriesPerBucket
                        && this.numBitsPerEntry == that.numBitsPerEntry
                        && this.size == that.size
                        && this.checksum == that.checksum
                        ;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(numBuckets, numEntriesPerBucket, numBitsPerEntry, size,
                    checksum);
        }

        public boolean isCompatible(CuckooTable that) {
            return this.numBuckets == that.numBuckets
                    && this.numEntriesPerBucket == that.numEntriesPerBucket
                    && this.numBitsPerEntry == that.numBitsPerEntry;
        }

        public long size() {
            return size;
        }

        public long checksum() {
            return checksum;
        }

        public long bitSize() {
            return (long) data.length * Long.SIZE;
        }

        public long capacity() {
            return numBuckets * numEntriesPerBucket;
        }

        public double load() {
            return (double) size() / (double) capacity();
        }

        public double expectedFpp() {
            return (2.0D * size / numBuckets) / Math.pow(2, numBitsPerEntry);
        }

        public double averageBitsPerEntry() {
            return (double) bitSize() / (double) size;
        }

        @Override
        public String toString() {
            return "CuckooTable{" +
                    "size=" + size +
                    ", checksum=" + checksum +
                    ", byteSize=" + bitSize() / Byte.SIZE +
                    ", load=" + load() +
                    ", capacity=" + capacity() +
                    ", expectedFpp=" + expectedFpp() +
                    ", averageBitsPerEntry=" + averageBitsPerEntry() +
                    ", numBuckets=" + numBuckets +
                    ", numEntriesPerBucket=" + numEntriesPerBucket +
                    ", numBitsPerEntry=" + numBitsPerEntry +
                    '}';
        }
    }

}

