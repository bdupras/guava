package com.google.common.hash;

import javax.annotation.CheckReturnValue;

public interface ProbabilisticFilter<T> {
  @CheckReturnValue
  boolean mightContain(T object);

  @CheckReturnValue
  boolean put(T object);

  @CheckReturnValue
  double expectedFpp();
}
