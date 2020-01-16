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
package io.prestosql.tpcds.column;

import io.prestosql.tpcds.Table;

import static io.prestosql.tpcds.Table.CUSTOMER_DEMOGRAPHICS;
import static io.prestosql.tpcds.column.ColumnTypes.IDENTIFIER;
import static io.prestosql.tpcds.column.ColumnTypes.INTEGER;
import static io.prestosql.tpcds.column.ColumnTypes.character;

public enum CustomerDemographicsColumn
        implements Column
{
    CD_DEMO_SK(IDENTIFIER),
    CD_GENDER(character(1)),
    CD_MARITAL_STATUS(character(1)),
    CD_EDUCATION_STATUS(character(20)),
    CD_PURCHASE_ESTIMATE(INTEGER),
    CD_CREDIT_RATING(character(10)),
    CD_DEP_COUNT(INTEGER),
    CD_DEP_EMPLOYED_COUNT(INTEGER),
    CD_DEP_COLLEGE_COUNT(INTEGER);

    private final ColumnType type;

    CustomerDemographicsColumn(ColumnType type)
    {
        this.type = type;
    }

    @Override
    public Table getTable()
    {
        return CUSTOMER_DEMOGRAPHICS;
    }

    @Override
    public String getName()
    {
        return name().toLowerCase();
    }

    @Override
    public ColumnType getType()
    {
        return type;
    }

    @Override
    public int getPosition()
    {
        return ordinal();
    }
}
