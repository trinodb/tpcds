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
import io.trino.tpcds.row.CatalogReturnsRow;
import io.trino.tpcds.row.CatalogSalesRow;
import io.trino.tpcds.row.TableRow;
import io.trino.tpcds.type.Pricing;

import static io.trino.tpcds.JoinKeyUtils.generateJoinKey;
import static io.trino.tpcds.Nulls.createNullBitMap;
import static io.trino.tpcds.Table.CATALOG_RETURNS;
import static io.trino.tpcds.Table.CUSTOMER;
import static io.trino.tpcds.Table.CUSTOMER_ADDRESS;
import static io.trino.tpcds.Table.CUSTOMER_DEMOGRAPHICS;
import static io.trino.tpcds.Table.DATE_DIM;
import static io.trino.tpcds.Table.HOUSEHOLD_DEMOGRAPHICS;
import static io.trino.tpcds.Table.REASON;
import static io.trino.tpcds.Table.SHIP_MODE;
import static io.trino.tpcds.Table.TIME_DIM;
import static io.trino.tpcds.Table.WAREHOUSE;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_NULLS;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_PRICING;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_REASON_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNED_DATE_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNED_TIME_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_ADDR_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_CDEMO_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_CUSTOMER_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_RETURNING_HDEMO_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_SHIP_MODE_SK;
import static io.trino.tpcds.generator.CatalogReturnsGeneratorColumn.CR_WAREHOUSE_SK;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomInt;
import static io.trino.tpcds.type.Pricing.generatePricingForReturnsTable;
import static java.util.Collections.emptyList;

public class CatalogReturnsRowGenerator
        extends AbstractRowGenerator
{
    public static final int RETURN_PERCENT = 10;

    public CatalogReturnsRowGenerator()
    {
        super(CATALOG_RETURNS);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        // The catalog returns table is a child of the catalog_sales table because you can only return things that have
        // already been purchased.  This method should only get called if we are generating the catalog_returns table
        // in isolation. Otherwise catalog_returns is generated during the generation of the catalog_sales table
        RowGeneratorResult salesAndReturnsResult = parentRowGenerator.generateRowAndChildRows(rowNumber, session, null, this);
        if (salesAndReturnsResult.getRowAndChildRows().size() == 2) {
            return new RowGeneratorResult(ImmutableList.of(salesAndReturnsResult.getRowAndChildRows().get(1)), salesAndReturnsResult.shouldEndRow());
        }
        else {
            return new RowGeneratorResult(emptyList(), salesAndReturnsResult.shouldEndRow());  // no return occurred for given sale
        }
    }

    public TableRow generateRow(Session session, CatalogSalesRow salesRow)
    {
        long nullBitMap = createNullBitMap(CATALOG_RETURNS, getRandomNumberStream(CR_NULLS));

        // some of the fields are conditionally taken from the sale
        Scaling scaling = session.getScaling();
        long crReturningCustomerSk = generateJoinKey(CR_RETURNING_CUSTOMER_SK, getRandomNumberStream(CR_RETURNING_CUSTOMER_SK), CUSTOMER, 2, scaling);
        long crReturningCdemoSk = generateJoinKey(CR_RETURNING_CDEMO_SK, getRandomNumberStream(CR_RETURNING_CDEMO_SK), CUSTOMER_DEMOGRAPHICS, 2, scaling);
        long crReturningHdemoSk = generateJoinKey(CR_RETURNING_HDEMO_SK, getRandomNumberStream(CR_RETURNING_HDEMO_SK), HOUSEHOLD_DEMOGRAPHICS, 2, scaling);
        long crReturningAddrSk = generateJoinKey(CR_RETURNING_ADDR_SK, getRandomNumberStream(CR_RETURNING_ADDR_SK), CUSTOMER_ADDRESS, 2, scaling);
        if (generateUniformRandomInt(0, 99, getRandomNumberStream(CR_RETURNING_CUSTOMER_SK)) < CatalogSalesRowGenerator.GIFT_PERCENTAGE) {
            crReturningCustomerSk = salesRow.getCsShipCustomerSk();
            crReturningCdemoSk = salesRow.getCsShipCdemoSk();
            // skip crReturningHdemoSk, since it doesn't exist on the sales record
            crReturningAddrSk = salesRow.getCsShipAddrSk();
        }

        Pricing salesPricing = salesRow.getCsPricing();
        int quantity = salesPricing.getQuantity();
        if (salesRow.getCsPricing().getQuantity() != -1) {
            quantity = generateUniformRandomInt(1, quantity, getRandomNumberStream(CR_PRICING));
        }
        Pricing crPricing = generatePricingForReturnsTable(CR_PRICING, getRandomNumberStream(CR_PRICING), quantity, salesPricing);

        return new CatalogReturnsRow(generateJoinKey(CR_RETURNED_DATE_SK, getRandomNumberStream(CR_RETURNED_DATE_SK), DATE_DIM, salesRow.getCsShipDateSk(), scaling), // items cannot be returned until  they are shipped
                generateJoinKey(CR_RETURNED_TIME_SK, getRandomNumberStream(CR_RETURNED_TIME_SK), TIME_DIM, 1, scaling),
                salesRow.getCsSoldItemSk(),
                salesRow.getCsBillCustomerSk(),
                salesRow.getCsBillCdemoSk(),
                salesRow.getCsBillHdemoSk(),
                salesRow.getCsBillAddrSk(),
                crReturningCustomerSk,
                crReturningCdemoSk,
                crReturningHdemoSk,
                crReturningAddrSk,
                salesRow.getCsCallCenterSk(),
                salesRow.getCsCatalogPageSk(),
                generateJoinKey(CR_SHIP_MODE_SK, getRandomNumberStream(CR_SHIP_MODE_SK), SHIP_MODE, 1, scaling),
                generateJoinKey(CR_WAREHOUSE_SK, getRandomNumberStream(CR_WAREHOUSE_SK), WAREHOUSE, 1, scaling),
                generateJoinKey(CR_REASON_SK, getRandomNumberStream(CR_REASON_SK), REASON, 1, scaling),
                salesRow.getCsOrderNumber(),
                crPricing,
                nullBitMap);
    }
}
