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

import io.prestosql.tpcds.Session;
import io.prestosql.tpcds.row.CustomerAddressRow;
import io.prestosql.tpcds.type.Address;

import static io.prestosql.tpcds.BusinessKeyGenerator.makeBusinessKey;
import static io.prestosql.tpcds.Nulls.createNullBitMap;
import static io.prestosql.tpcds.Table.CUSTOMER_ADDRESS;
import static io.prestosql.tpcds.distribution.LocationTypesDistribution.LocationTypeWeights.UNIFORM;
import static io.prestosql.tpcds.distribution.LocationTypesDistribution.pickRandomLocationType;
import static io.prestosql.tpcds.generator.CustomerAddressGeneratorColumn.CA_ADDRESS;
import static io.prestosql.tpcds.generator.CustomerAddressGeneratorColumn.CA_LOCATION_TYPE;
import static io.prestosql.tpcds.generator.CustomerAddressGeneratorColumn.CA_NULLS;
import static io.prestosql.tpcds.type.Address.makeAddressForColumn;

public class CustomerAddressRowGenerator
        extends AbstractRowGenerator
{
    public CustomerAddressRowGenerator()
    {
        super(CUSTOMER_ADDRESS);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        long nullBitMap = createNullBitMap(CUSTOMER_ADDRESS, getRandomNumberStream(CA_NULLS));
        long caAddrSk = rowNumber;
        String caAddrId = makeBusinessKey(rowNumber);
        Address caAddr = makeAddressForColumn(CUSTOMER_ADDRESS, getRandomNumberStream(CA_ADDRESS), session.getScaling());
        String caLocationType = pickRandomLocationType(getRandomNumberStream(CA_LOCATION_TYPE), UNIFORM);
        return new RowGeneratorResult(new CustomerAddressRow(nullBitMap, caAddrSk, caAddrId, caAddr, caLocationType));
    }
}
