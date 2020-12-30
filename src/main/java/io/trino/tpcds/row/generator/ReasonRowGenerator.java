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

package io.trino.tpcds.row.generator;

import io.trino.tpcds.Session;
import io.trino.tpcds.row.ReasonRow;

import static io.trino.tpcds.BusinessKeyGenerator.makeBusinessKey;
import static io.trino.tpcds.Nulls.createNullBitMap;
import static io.trino.tpcds.Table.REASON;
import static io.trino.tpcds.distribution.ReturnReasonsDistribution.getReturnReasonAtIndex;
import static io.trino.tpcds.generator.ReasonGeneratorColumn.R_NULLS;

public class ReasonRowGenerator
        extends AbstractRowGenerator
{
    public ReasonRowGenerator()
    {
        super(REASON);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        long nullBitMap = createNullBitMap(REASON, getRandomNumberStream(R_NULLS));
        long rReasonSk = rowNumber;
        String rReasonId = makeBusinessKey(rowNumber);
        String rReasonDescription = getReturnReasonAtIndex((int) (rowNumber - 1));

        return new RowGeneratorResult(new ReasonRow(nullBitMap, rReasonSk, rReasonId, rReasonDescription));
    }
}
