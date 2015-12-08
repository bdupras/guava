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

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

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
        private final HashFunction hashFunction = Hashing.murmur3_128();

        @Override
        public <T> boolean put(T object, Funnel<? super T> funnel, long numBuckets, int numEntriesPerBucket, int numBitsPerEntry, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, numBitsPerEntry);
            final long index1 = index(hash1, numBuckets);
            final long index2 = altIndex(index1, fingerprint, numBuckets);
            // TODO: balance by vacancies?
            return table.putEntry(index1, fingerprint) || table.putEntry(index2, fingerprint);
        }

        @Override
        public <T> boolean delete(T object, Funnel<? super T> funnel, long numBuckets, int numEntriesPerBucket, int numBitsPerEntry, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, numBitsPerEntry);
            final long index1 = index(hash1, numBuckets);
            final long index2 = altIndex(index1, fingerprint, numBuckets);
            return table.deleteEntry(index1, fingerprint) || table.deleteEntry(index2, fingerprint);
        }

        @Override
        public <T> boolean mightContain(T object, Funnel<? super T> funnel, long numBuckets, int numEntriesPerBucket, int numBitsPerEntry, CuckooTable table) {
            final long hash64 = hash(object, funnel).asLong();
            final int hash1 = hash1(hash64);
            final int hash2 = hash2(hash64);
            final int fingerprint = fingerprint(hash2, numBitsPerEntry);
            final long index1 = index(hash1, numBuckets);
            final long index2 = altIndex(index1, fingerprint, numBuckets);
            return table.hasEntry(index1, fingerprint) || table.hasEntry(index2, fingerprint);
        }

        <T> HashCode hash(T object, Funnel<? super T> funnel) {
            return hashFunction.hashObject(object, funnel);
        }

        private int hash1(long hash64) {
            return (int)hash64;
        }

        private int hash2(long hash64) {
            return (int) (hash64 >>> 32);
        }

        /**
         * Returns an f-bit portion of the given hash. Iteraing by b-bit segments from the least
         * significant side of the has to the most significant, looks for a non-zero segment. If a
         * non-zero segment isn't found, 1 is returned to distinguish the fingerprint from a
         * non-entry.
         *
         * @param hash 32-bit hash value
         * @param f number of bits to consider from the hash
         * @return first non-zero f-bit value from hash as an int, or 1 if no non-zero value is found
         */
        int fingerprint(int hash, int f) {
            for (int i = Integer.SIZE/f; i>=1; i--) {
                int mask = ((1 << Integer.SIZE-1) >> f) >>> ((i-1) * f);
                int fingerprint = (hash & mask) >>> Integer.SIZE-(i*f);
                if (0 != fingerprint) {
                    return fingerprint;
                }
            }
            return -1;
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
        public long index(int hash, long m) {
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
         *       sum with starting index
         *         and modulo to 0..m-1
         *
         * (*) Constraining the CuckooTable to an even size in buckets, and applying odd offsets
         *     guarantees opposite parities for index & altIndex. The parity of the starting index
         *     determines whether the offset is subtracted from or added to the starting index.
         *     This strategy guarantees altIndex() is reversible, i.e.
         *
         *       index == altIndex(altIndex(index, fingerprint, m), fingerprint, m)
         *
         * @param index starting index
         * @param fingerprint fingerprint
         * @param m size of table in buckets; must be even for this strategy
         * @return an alternate index for fingerprint bounded by 0..m-1
         */
        @Override
        public long altIndex(long index, int fingerprint, long m) {
            return mod((index + (parsign(index) * odd(hash(fingerprint)))), m);
        }

        /**
         * Return value is -1 if the specified value is odd and +1 if the specified value is even.
         * @return -1 if input is odd, 1 if input is even
         */
        private long parsign(long i) {
            return ((i & 0x01L) * 2L) - 1L;
        }

        int hash(int i) {
            return hashFunction.hashInt(i).asInt();
        }

        long odd(long i) {
            return i | 0x01L;
        }
    };

    public abstract long index(int hash, long m);

    public abstract long altIndex(long index, int fingerprint, long m);

    static class CuckooTable extends BloomFilterStrategies.BitArray {
        long entryCount;
        final long numBuckets;
        final int numEntriesPerBucket;
        final int numBitsPerEntry;

        CuckooTable(long numBuckets, int numEntriesPerBucket, int numBitsPerEntry) {
            this(new long[Ints.checkedCast(LongMath.divide(
                    LongMath.checkedMultiply(numBuckets,
                            LongMath.checkedMultiply(numEntriesPerBucket, numBitsPerEntry)),
                    64, RoundingMode.CEILING))]
                    , 0L
                    , numBuckets
                    , numEntriesPerBucket
                    , numBitsPerEntry
            );
        }

        CuckooTable(long[] data, long entryCount, long numBuckets, int numEntriesPerBucket, int numBitsPerEntry) {
            super(data);
            this.entryCount = entryCount;
            this.numBuckets = numBuckets;
            this.numEntriesPerBucket = numEntriesPerBucket;
            this.numBitsPerEntry = numBitsPerEntry;
        }

        CuckooTable copy() {
            return new CuckooTable(data.clone(), entryCount, numBuckets, numEntriesPerBucket, numBitsPerEntry);
        }

        public boolean putEntry(long index, int entry) {
            boolean ret = false;
            if (ret) {
                entryCount++;
            }
            return ret;
        }

        public boolean deleteEntry(long index, int entry) {
            boolean ret = false;
            if (ret) {
                entryCount--;
            }
            return ret;
        }

        public boolean hasEntry(long index, int entry) {
            return false;
        }

//           63     ...     16      8       0
//           -------+-------+-------+-------|
//
//      [22] -------+-------+-------+-----yyy
//           -------+-------+-------+-----111
//
//      [21] xxxxx--+-------+-------+-------|
//           11111--+-------+-------+-------|
//
//      [20] -------+-------+---yyyxxxxx----|
//           -------+-------+---11111111----|


//        @SuppressWarnings("NumericOverflow")
//        int get(long index, int bitCount) {
//            final int firstIndex = (int) (index >>> 6);
//            final int secondIndex = (int) ((index + bitCount) >>> 6);
//            if (firstIndex == secondIndex) {
//                // shift right and mask off left
//            } else {
//                final long lower = data[firstIndex] >>> (Long.SIZE-lowerBitCount);
//
//                final long upperMask = -1L >>> (Long.SIZE - upperBitCount);
//                final long upper = ( data[firstIndex] & upperMask ) << lowerBitCount;
//
//                Math.pow()
//                return (int) (lower | upper);
//
//            }
//
//        }
    }

}

