/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;

/** TODO. */
@PublicEvolving
public interface ListState<T> extends State {

    /**
     * Returns the current iterator for the state asynchronously. When the state is not partitioned
     * the returned value is the same for all inputs in a given operator instance. If state
     * partitioning is applied, the value returned depends on the current operator input, as the
     * operator maintains an independent state for each partition. This method will never return
     * (@code null).
     *
     * @return The {@link StateFuture} that will return list iterator corresponding to the current
     *     input.
     */
    StateFuture<StateIterator<T>> asyncGet();

    /**
     * Updates the operator state accessible by {@link #asyncGet()} by adding the given value to the
     * list of values. The next time {@link #asyncGet()} is called (for the same state partition)
     * the returned state will represent the updated list.
     *
     * <p>Null is not allowed to be passed in
     *
     * @param value The new value for the state.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncAdd(T value);

    /**
     * Updates the operator state accessible by {@link #asyncGet()} by updating existing values to
     * the given list of values. The next time {@link #asyncGet()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>Null value passed in or any null value in list is not allowed.
     *
     * @param values The new value list for the state.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncUpdate(List<T> values);

    /**
     * Updates the operator state accessible by {@link #asyncGet()} by adding the given values to
     * the list of values. The next time {@link #asyncGet()} is called (for the same state
     * partition) the returned state will represent the updated list.
     *
     * <p>Null value passed in or any null value in list is not allowed.
     *
     * @param values The new values to add for the state.
     * @return The {@link StateFuture} that will trigger the callback when update finishes.
     */
    StateFuture<Void> asyncAddAll(List<T> values);
}
