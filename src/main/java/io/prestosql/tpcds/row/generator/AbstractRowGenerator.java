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

package io.prestosql.tpcds.row.generator;

import com.google.common.collect.ImmutableMap;
import io.prestosql.tpcds.Table;
import io.prestosql.tpcds.generator.GeneratorColumn;
import io.prestosql.tpcds.random.RandomNumberStream;
import io.prestosql.tpcds.random.RandomNumberStreamImpl;

import static io.prestosql.tpcds.random.RandomValueGenerator.generateUniformRandomInt;

public abstract class AbstractRowGenerator
        implements RowGenerator
{
    private final ImmutableMap<GeneratorColumn, RandomNumberStream> randomNumberStreamMap;

    public AbstractRowGenerator(Table table)
    {
        ImmutableMap.Builder<GeneratorColumn, RandomNumberStream> mapBuilder = ImmutableMap.builder();
        for (GeneratorColumn column : table.getGeneratorColumns()) {
            mapBuilder.put(column, new RandomNumberStreamImpl(column.getGlobalColumnNumber(), column.getSeedsPerRow()));
        }
        randomNumberStreamMap = mapBuilder.build();
    }

    @Override
    public void consumeRemainingSeedsForRow()
    {
        for (RandomNumberStream randomNumberStream : randomNumberStreamMap.values()) {
            while (randomNumberStream.getSeedsUsed() < randomNumberStream.getSeedsPerRow()) {
                generateUniformRandomInt(1, 100, randomNumberStream);
            }
            randomNumberStream.resetSeedsUsed();
        }
    }

    public void skipRowsUntilStartingRowNumber(long startingRowNumber)
    {
        for (RandomNumberStream randomNumberStream : randomNumberStreamMap.values()) {
            randomNumberStream.skipRows((int) startingRowNumber - 1);  // casting long to int copies C code
        }
    }

    public RandomNumberStream getRandomNumberStream(GeneratorColumn column)
    {
        return randomNumberStreamMap.get(column);
    }
}
