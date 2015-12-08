/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.common.hash;

import junit.framework.TestCase;

import static com.google.common.hash.CuckooFilterStrategies.*;
import static com.google.common.truth.Truth.assertThat;

/**
 * Tests for CuckooFilterStrategies.
 *
 * @author Brian Dupras
 */
public class CuckooFilterStrategiesTest extends TestCase {
    public void testSummen() throws Exception {
        int numInsertions = 1000000;
//        CuckooFilter<String> filter = CuckooFilter.create(
//                Funnels.longFunnel(), numInsertions, 0.03,
//                CuckooFilterStrategies.MURMUR128_BEALDUPRAS_32);
    }

    public void testAltIndex() throws Exception {
        for (long i = 0; i < Integer.MAX_VALUE; i++) {
            assertTrue(-1L < MURMUR128_BEALDUPRAS_32.index((int)i, Long.MAX_VALUE));
            assertTrue(-1L < MURMUR128_BEALDUPRAS_32.index((int)i, Integer.MAX_VALUE));
        }
    }

    /**
     * This test will fail whenever someone updates/reorders the BloomFilterStrategies constants.
     * Only appending a new constant is allowed.
     */
    public void testBloomFilterStrategies() {
        assertThat(values()).hasLength(1);
        assertEquals(MURMUR128_BEALDUPRAS_32, values()[0]);
    }
}
