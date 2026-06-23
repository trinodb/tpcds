/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.tpcds.distribution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.tpcds.TpcdsException;
import io.trino.tpcds.random.RandomNumberStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomInt;

public final class DistributionUtils
{
    // Split on a separator that is not escaped with a backslash. Compiled once and reused.
    private static final Pattern COLON_SEPARATOR = Pattern.compile("(?<!\\\\):");
    private static final Pattern COMMA_SEPARATOR = Pattern.compile("(?<!\\\\),");

    private DistributionUtils() {}

    protected static final class WeightsBuilder
    {
        ImmutableList.Builder<Integer> weightsBuilder = ImmutableList.builder();
        int previousWeight;

        public WeightsBuilder computeAndAddNextWeight(int weight)
        {
            checkArgument(weight >= 0, "Weight cannot be negative.");
            int newWeight = previousWeight + weight;
            weightsBuilder.add(newWeight);
            previousWeight = newWeight;
            return this;
        }

        public ImmutableList<Integer> build()
        {
            return weightsBuilder.build();
        }
    }

    protected static Iterator<List<String>> getDistributionIterator(String filename)
    {
        URL resource = Resources.getResource(DistributionUtils.class, filename);
        checkState(resource != null, "Distribution file '%s' not found", filename);
        try {
            // get an iterator that iterates over lists of the colon separated values from the distribution files
            return transform(
                    filter(Resources.asCharSource(resource, StandardCharsets.ISO_8859_1).readLines().iterator(), line -> {
                        line = line.trim();
                        return !line.isEmpty() && !line.startsWith("--");
                    }), line -> ImmutableList.copyOf(Splitter.on(COLON_SEPARATOR).trimResults().split(line)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static List<String> getListFromCommaSeparatedValues(String toSplit)
    {
        List<String> values = Splitter.on(COMMA_SEPARATOR).trimResults().splitToList(toSplit);
        return values.stream().map(value -> value.replace("\\", "")).collect(Collectors.toList());
    }

    protected static <T> T pickRandomValue(List<T> values, List<Integer> weights, RandomNumberStream randomNumberStream)
    {
        int weight = generateUniformRandomInt(1, weights.get(weights.size() - 1), randomNumberStream);
        return getValueForWeight(weight, values, weights);
    }

    private static <T> T getValueForWeight(int weight, List<T> values, List<Integer> weights)
    {
        checkArgument(values.size() == weights.size());
        return values.get(getIndexForWeight(weight, weights));
    }

    protected static <T> T getValueForIndexModSize(long index, List<T> values)
    {
        int size = values.size();
        int indexModSize = (int) (index % size);
        return values.get(indexModSize);
    }

    protected static int pickRandomIndex(List<Integer> weights, RandomNumberStream randomNumberStream)
    {
        int weight = generateUniformRandomInt(1, weights.get(weights.size() - 1), randomNumberStream);
        return getIndexForWeight(weight, weights);
    }

    private static int getIndexForWeight(int weight, List<Integer> weights)
    {
        // The weights are prefix sums (monotonically non-decreasing), so the smallest index whose
        // cumulative weight is >= the target is a lower-bound binary search. This returns the exact
        // same index as a linear scan, including when consecutive weights are equal.
        int low = 0;
        int high = weights.size() - 1;
        int result = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (weight <= weights.get(mid)) {
                result = mid;
                high = mid - 1;
            }
            else {
                low = mid + 1;
            }
        }

        if (result == -1) {
            throw new TpcdsException("random weight was greater than max weight");
        }
        return result;
    }

    protected static int getWeightForIndex(int index, List<Integer> weights)
    {
        checkArgument(index < weights.size(), "index larger than distribution");
        return index == 0 ? weights.get(index) : weights.get(index) - weights.get(index - 1);  // reverse the accumulation of weights.
    }
}
