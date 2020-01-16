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
import io.prestosql.tpcds.row.CustomerDemographicsRow;

import static io.prestosql.tpcds.Nulls.createNullBitMap;
import static io.prestosql.tpcds.Table.CUSTOMER_DEMOGRAPHICS;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.CREDIT_RATING_DISTRIBUTION;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.EDUCATION_DISTRIBUTION;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.GENDER_DISTRIBUTION;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.MARITAL_STATUS_DISTRIBUTION;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.PURCHASE_BAND_DISTRIBUTION;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.getCreditRatingForIndexModSize;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.getEducationForIndexModSize;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.getGenderForIndexModSize;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.getMaritalStatusForIndexModSize;
import static io.prestosql.tpcds.distribution.DemographicsDistributions.getPurchaseBandForIndexModSize;
import static io.prestosql.tpcds.generator.CustomerDemographicsGeneratorColumn.CD_NULLS;

public class CustomerDemographicsRowGenerator
        extends AbstractRowGenerator
{
    private static final int MAX_CHILDREN = 7;
    private static final int MAX_EMPLOYED = 7;
    private static final int MAX_COLLEGE = 7;

    public CustomerDemographicsRowGenerator()
    {
        super(CUSTOMER_DEMOGRAPHICS);
    }

    @Override
    public RowGeneratorResult generateRowAndChildRows(long rowNumber, Session session, RowGenerator parentRowGenerator, RowGenerator childRowGenerator)
    {
        long nullBitMap = createNullBitMap(CUSTOMER_DEMOGRAPHICS, getRandomNumberStream(CD_NULLS));
        long cDemoSk = rowNumber;
        long index = cDemoSk - 1;

        String cdGender = getGenderForIndexModSize(index);
        index = index / GENDER_DISTRIBUTION.getSize();
        String cdMaritalStatus = getMaritalStatusForIndexModSize(index);

        index = index / MARITAL_STATUS_DISTRIBUTION.getSize();
        String cdEducationStatus = getEducationForIndexModSize(index);

        index = index / EDUCATION_DISTRIBUTION.getSize();
        int cdPurchaseEstimate = getPurchaseBandForIndexModSize(index);

        index = index / PURCHASE_BAND_DISTRIBUTION.getSize();
        String cdCreditRating = getCreditRatingForIndexModSize(index);

        index = index / CREDIT_RATING_DISTRIBUTION.getSize();
        int cdDepCount = (int) (index % (long) MAX_CHILDREN);

        index /= (long) MAX_CHILDREN;
        int cdEmployedCount = (int) (index % (long) MAX_EMPLOYED);

        index /= (long) MAX_EMPLOYED;
        int cdDepCollegeCount = (int) (index % (long) MAX_COLLEGE);

        return new RowGeneratorResult(new CustomerDemographicsRow(nullBitMap,
                cDemoSk,
                cdGender,
                cdMaritalStatus,
                cdEducationStatus,
                cdPurchaseEstimate,
                cdCreditRating,
                cdDepCount,
                cdEmployedCount,
                cdDepCollegeCount));
    }
}
