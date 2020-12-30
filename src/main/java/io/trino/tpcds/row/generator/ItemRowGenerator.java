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
import io.trino.tpcds.SlowlyChangingDimensionUtils.SlowlyChangingDimensionKey;
import io.trino.tpcds.distribution.CategoriesDistribution;
import io.trino.tpcds.distribution.CategoryClassDistributions.CategoryClass;
import io.trino.tpcds.distribution.ItemsDistributions;
import io.trino.tpcds.row.ItemRow;
import io.trino.tpcds.type.Decimal;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.Optional;

import static io.trino.tpcds.JoinKeyUtils.generateJoinKey;
import static io.trino.tpcds.Nulls.createNullBitMap;
import static io.trino.tpcds.SlowlyChangingDimensionUtils.computeScdKey;
import static io.trino.tpcds.SlowlyChangingDimensionUtils.getValueForSlowlyChangingDimension;
import static io.trino.tpcds.Table.ITEM;
import static io.trino.tpcds.Table.PROMOTION;
import static io.trino.tpcds.distribution.CategoriesDistribution.getCategoryAtIndex;
import static io.trino.tpcds.distribution.CategoriesDistribution.getHasSizeAtIndex;
import static io.trino.tpcds.distribution.CategoryClassDistributions.pickRandomCategoryClass;
import static io.trino.tpcds.distribution.EnglishDistributions.SYLLABLES_DISTRIBUTION;
import static io.trino.tpcds.distribution.ItemCurrentPriceDistribution.pickRandomCurrentPriceRange;
import static io.trino.tpcds.distribution.ItemsDistributions.BRAND_SYLLABLES_DISTRIBUTION;
import static io.trino.tpcds.distribution.ItemsDistributions.ColorsWeights.SKEWED;
import static io.trino.tpcds.distribution.ItemsDistributions.IdWeights.UNIFIED;
import static io.trino.tpcds.distribution.ItemsDistributions.SizeWeights.NO_SIZE;
import static io.trino.tpcds.distribution.ItemsDistributions.SizeWeights.SIZED;
import static io.trino.tpcds.distribution.ItemsDistributions.pickRandomColor;
import static io.trino.tpcds.distribution.ItemsDistributions.pickRandomManufactIdRange;
import static io.trino.tpcds.distribution.ItemsDistributions.pickRandomSize;
import static io.trino.tpcds.distribution.ItemsDistributions.pickRandomUnit;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CATEGORY;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CLASS;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_COLOR;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_CURRENT_PRICE;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_FORMULATION;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_ITEM_DESC;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_MANAGER_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_MANUFACT_ID;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_NULLS;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_PROMO_SK;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_SCD;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_SIZE;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_UNITS;
import static io.trino.tpcds.generator.ItemGeneratorColumn.I_WHOLESALE_COST;
import static io.trino.tpcds.random.RandomValueGenerator.DIGITS;
import static io.trino.tpcds.random.RandomValueGenerator.generateRandomCharset;
import static io.trino.tpcds.random.RandomValueGenerator.generateRandomText;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomDecimal;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomInt;
import static io.trino.tpcds.random.RandomValueGenerator.generateUniformRandomKey;
import static io.trino.tpcds.random.RandomValueGenerator.generateWord;
import static io.trino.tpcds.type.Decimal.multiply;
import static java.lang.String.format;

@NotThreadSafe
public class ItemRowGenerator
        extends AbstractRowGenerator
{
    private static final Decimal MIN_ITEM_MARKDOWN_PCT = new Decimal(30, 2);
    private static final Decimal MAX_ITEM_MARKDOWN_PCT = new Decimal(90, 2);
    private static final int ROW_SIZE_I_PRODUCT_NAME = 50;
    private static final int ROW_SIZE_I_ITEM_DESC = 200;
    private static final int ROW_SIZE_I_MANUFACT = 50;
    private static final int ROW_SIZE_I_FORMULATION = 20;
    private static final int I_PROMO_PERCENTAGE = 20;

    private Optional<ItemRow> previousRow = Optional.empty();

    public ItemRowGenerator()
    {
        super(ITEM);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        long nullBitMap = createNullBitMap(ITEM, getRandomNumberStream(I_NULLS));
        long iItemSk = rowNumber;

        List<Integer> managerIdRange = ItemsDistributions.pickRandomManagerIdRange(UNIFIED, getRandomNumberStream(I_MANAGER_ID));
        long iManagerId = generateUniformRandomKey(managerIdRange.get(0), managerIdRange.get(1), getRandomNumberStream(I_MANAGER_ID));

        SlowlyChangingDimensionKey slowlyChangingDimensionKey = computeScdKey(ITEM, rowNumber);
        String iItemId = slowlyChangingDimensionKey.getBusinessKey();
        long iRecStartDateId = slowlyChangingDimensionKey.getStartDate();
        long iRecEndDateId = slowlyChangingDimensionKey.getEndDate();
        boolean isNewBusinessKey = slowlyChangingDimensionKey.isNewBusinessKey();

        // select the random number that controls if a field changes from
        // one record to the next.
        int fieldChangeFlags = (int) getRandomNumberStream(I_SCD).nextRandom();

        // the rest of the record in a history-keeping dimension can either be a new data value or not;
        // use a random number and its bit pattern to determine which fields to replace and which to retain
        String iItemDesc = generateRandomText(1, ROW_SIZE_I_ITEM_DESC, getRandomNumberStream(I_ITEM_DESC));
        if (previousRow.isPresent()) {
            iItemDesc = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiItemDesc(), iItemDesc);
        }
        fieldChangeFlags >>= 1;

        // There is a bug in the C code that always chooses the new record for current price
        List<Decimal> currentPriceRange = pickRandomCurrentPriceRange(getRandomNumberStream(I_CURRENT_PRICE));
        Decimal iCurrentPrice = generateUniformRandomDecimal(currentPriceRange.get(0), currentPriceRange.get(1), getRandomNumberStream(I_CURRENT_PRICE));
        fieldChangeFlags >>= 1;

        Decimal markdown = generateUniformRandomDecimal(MIN_ITEM_MARKDOWN_PCT, MAX_ITEM_MARKDOWN_PCT, getRandomNumberStream(I_WHOLESALE_COST));
        Decimal iWholesaleCost = multiply(iCurrentPrice, markdown);
        if (previousRow.isPresent()) {
            iWholesaleCost = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiWholesaleCost(), iWholesaleCost);
        }
        fieldChangeFlags >>= 1;

        int iCategoryIndex = CategoriesDistribution.pickRandomIndex(getRandomNumberStream(I_CATEGORY));
        int iCategoryId = iCategoryIndex + 1;
        String iCategory = getCategoryAtIndex(iCategoryIndex);

        CategoryClass categoryClass = pickRandomCategoryClass(iCategoryIndex, getRandomNumberStream(I_CLASS));
        String iClass = categoryClass.getName();
        long newClassId = categoryClass.getId();
        long iClassId = newClassId;
        if (previousRow.isPresent()) {
            iClassId = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiClassId(), iClassId);
        }
        fieldChangeFlags >>= 1;

        int brandCount = categoryClass.getBrandCount();
        long iBrandId = rowNumber % brandCount + 1;
        String iBrand = generateWord(iCategoryId * 10 + newClassId, 45, BRAND_SYLLABLES_DISTRIBUTION);
        iBrand += format(" #%d", iBrandId);
        iBrandId += (iCategoryId * 1000 + newClassId) * 1000;
        if (previousRow.isPresent()) {
            iBrandId = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiBrandId(), iBrandId);
        }
        fieldChangeFlags >>= 1;

        // always uses a new value due to a bug in the C code
        int hasSize = getHasSizeAtIndex(iCategoryIndex);
        String iSize = pickRandomSize(hasSize == 0 ? NO_SIZE : SIZED, getRandomNumberStream(I_SIZE));
        fieldChangeFlags >>= 1;

        List<Integer> manufactIdRange = pickRandomManufactIdRange(UNIFIED, getRandomNumberStream(I_MANUFACT_ID));
        long iManufactId = generateUniformRandomInt(manufactIdRange.get(0), manufactIdRange.get(1), getRandomNumberStream(I_MANUFACT_ID));
        if (previousRow.isPresent()) {
            iManufactId = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiManufactId(), iManufactId);
        }
        fieldChangeFlags >>= 1;

        String iManufact = generateWord(iManufactId, ROW_SIZE_I_MANUFACT, SYLLABLES_DISTRIBUTION);
        if (previousRow.isPresent()) {
            iManufact = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiManufact(), iManufact);
        }
        fieldChangeFlags >>= 1;

        String iFormulation = generateRandomCharset(DIGITS, ROW_SIZE_I_FORMULATION, ROW_SIZE_I_FORMULATION, getRandomNumberStream(I_FORMULATION));
        String color = pickRandomColor(SKEWED, getRandomNumberStream(I_FORMULATION));
        int position = generateUniformRandomInt(0, iFormulation.length() - color.length() - 1, getRandomNumberStream(I_FORMULATION));
        StringBuilder builder = new StringBuilder(iFormulation);
        builder.replace(position, color.length() + position, color);
        iFormulation = builder.toString();
        if (previousRow.isPresent()) {
            iFormulation = getValueForSlowlyChangingDimension(fieldChangeFlags, isNewBusinessKey, previousRow.get().getiFormulation(), iFormulation);
        }

        // these fields always use a new value due to a bug in the C code
        String iColor = pickRandomColor(SKEWED, getRandomNumberStream(I_COLOR));
        String iUnits = pickRandomUnit(getRandomNumberStream(I_UNITS));
        String iContainer = "Unknown";
        String iProductName = generateWord(rowNumber, ROW_SIZE_I_PRODUCT_NAME, SYLLABLES_DISTRIBUTION);

        long iPromoSk = generateJoinKey(I_PROMO_SK, getRandomNumberStream(I_PROMO_SK), PROMOTION, 1, session.getScaling());
        int temp = generateUniformRandomInt(1, 100, getRandomNumberStream(I_PROMO_SK));
        if (temp > I_PROMO_PERCENTAGE) {
            iPromoSk = -1;
        }

        ItemRow row = new ItemRow(nullBitMap,
                iItemSk,
                iItemId,
                iRecStartDateId,
                iRecEndDateId,
                iItemDesc,
                iCurrentPrice,
                iWholesaleCost,
                iBrandId,
                iBrand,
                iClassId,
                iClass,
                iCategoryId,
                iCategory,
                iManufactId,
                iManufact,
                iSize,
                iFormulation,
                iColor,
                iUnits,
                iContainer,
                iManagerId,
                iProductName,
                iPromoSk);
        previousRow = Optional.of(row);
        return new RowGeneratorResult(row);
    }
}
