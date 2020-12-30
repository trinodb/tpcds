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

import com.google.common.collect.ImmutableList;
import io.trino.tpcds.Scaling;
import io.trino.tpcds.Session;
import io.trino.tpcds.row.StoreReturnsRow;
import io.trino.tpcds.row.StoreSalesRow;
import io.trino.tpcds.row.TableRow;
import io.trino.tpcds.type.Pricing;

import static io.trino.tpcds.JoinKeyUtils.generateJoinKey;
import static io.trino.tpcds.Nulls.createNullBitMap;
import static io.trino.tpcds.Table.CUSTOMER;
import static io.trino.tpcds.Table.CUSTOMER_ADDRESS;
import static io.trino.tpcds.Table.CUSTOMER_DEMOGRAPHICS;
import static io.trino.tpcds.Table.DATE_DIM;
import static io.trino.tpcds.Table.HOUSEHOLD_DEMOGRAPHICS;
import static io.trino.tpcds.Table.REASON;
import static io.trino.tpcds.Table.STORE;
import static io.trino.tpcds.Table.STORE_RETURNS;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_ADDR_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_CDEMO_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_CUSTOMER_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_HDEMO_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_NULLS;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_PRICING;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_REASON_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_RETURNED_DATE_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_RETURNED_TIME_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_STORE_SK;
import static io.trino.tpcds.generator.StoreReturnsGeneratorColumn.SR_TICKET_NUMBER;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomInt;
import static io.trino.tpcds.type.Pricing.generatePricingForReturnsTable;
import static java.util.Collections.emptyList;

public class StoreReturnsRowGenerator
        extends AbstractRowGenerator
{
    private static final int SR_SAME_CUSTOMER = 80;

    public StoreReturnsRowGenerator()
    {
        super(STORE_RETURNS);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        // The store_returns table is a child of the store_sales table because you can only return things that have
        // already been purchased.  This method should only get called if we are generating the store_returns table
        // in isolation. Otherwise store_returns is generated during the generation of the store_sales table
        RowGeneratorResult salesAndReturnsResult = parentRowGenerator.generateRowAndChildRows(rowNumber, session, null, this);
        if (salesAndReturnsResult.getRowAndChildRows().size() == 2) {
            return new RowGeneratorResult(ImmutableList.of(salesAndReturnsResult.getRowAndChildRows().get(1)), salesAndReturnsResult.shouldEndRow());
        }
        else {
            return new RowGeneratorResult(emptyList(), salesAndReturnsResult.shouldEndRow());  // no return occurred for given sale
        }
    }

    public TableRow generateRow(Session session, StoreSalesRow salesRow)
    {
        long nullBitMap = createNullBitMap(STORE_RETURNS, getRandomNumberStream(SR_NULLS));

        // some of the information in the return is taken from the original sale
        long srTicketNumber = salesRow.getSsTicketNumber();
        long srItemSk = salesRow.getSsSoldItemSk();

        // some of the fields are conditionally taken from the sale
        Scaling scaling = session.getScaling();
        long srCustomerSk = generateJoinKey(SR_CUSTOMER_SK, getRandomNumberStream(SR_CUSTOMER_SK), CUSTOMER, 1, scaling);
        int randomInt = generateUniformRandomInt(1, 100, getRandomNumberStream(SR_TICKET_NUMBER));
        if (randomInt < SR_SAME_CUSTOMER) {
            srCustomerSk = salesRow.getSsSoldCustomerSk();
        }

        // the rest of the columns are generated for this specific return
        long srReturnedDateSk = generateJoinKey(SR_RETURNED_DATE_SK, getRandomNumberStream(SR_RETURNED_DATE_SK), DATE_DIM, salesRow.getSsSoldDateSk(), scaling);
        long srReturnedTimeSk = generateUniformRandomInt(8 * 3600 - 1, 17 * 3600 - 1, getRandomNumberStream(SR_RETURNED_TIME_SK));
        long srCdemoSk = generateJoinKey(SR_CDEMO_SK, getRandomNumberStream(SR_CDEMO_SK), CUSTOMER_DEMOGRAPHICS, 1, scaling);
        long srHdemoSk = generateJoinKey(SR_HDEMO_SK, getRandomNumberStream(SR_HDEMO_SK), HOUSEHOLD_DEMOGRAPHICS, 1, scaling);
        long srAddrSk = generateJoinKey(SR_ADDR_SK, getRandomNumberStream(SR_ADDR_SK), CUSTOMER_ADDRESS, 1, scaling);
        long srStoreSk = generateJoinKey(SR_STORE_SK, getRandomNumberStream(SR_STORE_SK), STORE, 1, scaling);
        long srReasonSk = generateJoinKey(SR_REASON_SK, getRandomNumberStream(SR_REASON_SK), REASON, 1, scaling);

        Pricing salesPricing = salesRow.getSsPricing();
        int quantity = generateUniformRandomInt(1, salesPricing.getQuantity(), getRandomNumberStream(SR_PRICING));
        Pricing srPricing = generatePricingForReturnsTable(SR_PRICING, getRandomNumberStream(SR_PRICING), quantity, salesPricing);

        return new StoreReturnsRow(nullBitMap,
                srReturnedDateSk,
                srReturnedTimeSk,
                srItemSk,
                srCustomerSk,
                srCdemoSk,
                srHdemoSk,
                srAddrSk,
                srStoreSk,
                srReasonSk,
                srTicketNumber,
                srPricing);
    }
}
