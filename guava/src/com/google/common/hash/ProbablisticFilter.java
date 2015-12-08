package com.google.common.hash;

import javax.annotation.CheckReturnValue;

public interface ProbablisticFilter<T> {
    @CheckReturnValue
    boolean mightContain(T object);

    boolean put(T object);

    @CheckReturnValue
    double expectedFpp();

    @CheckReturnValue
    boolean isCompatible(BloomFilter<T> that);
}
