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

package io.trino.tpcds;

import io.trino.tpcds.Parallel.ChunkBoundaries;
import org.testng.annotations.Test;

import static io.trino.tpcds.GeneratorAssertions.assertPartialMD5;
import static io.trino.tpcds.Parallel.splitWork;
import static io.trino.tpcds.Session.getDefaultSession;
import static io.trino.tpcds.Table.CUSTOMER;

public class TestCustomerGenerator
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(CUSTOMER);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(CUSTOMER), CUSTOMER, session, "d7fbf74d3a6902abc28fd90d2cf6e0d9");
    }

    @Test
    public void testScaleFactor1()
    {
        assertPartialMD5(1, TEST_SESSION.getScaling().getRowCount(CUSTOMER), CUSTOMER, TEST_SESSION, "a08066ed04041d3370f923a9a3969900");
    }

    @Test
    public void testScaleFactor10()
    {
        Session session = TEST_SESSION.withScale(10);
        assertPartialMD5(1, session.getScaling().getRowCount(CUSTOMER), CUSTOMER, session, "7842d4e8c9fc489c8c9ad7cf6d6064af");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(100).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "c962b75bedf11b6d0b2f3ab4c7b2b20a");

        session = session.withChunkNumber(50);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "d5f8b7401f398fd2f9bee77e33cf1350");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "01dac32e9b5c99f66c51433b7a5e6738");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(300).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "514112b6a3c44f91439fd010f0a579f9");

        session = session.withChunkNumber(50);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "36f3f11ff1b86798c8b67df347778551");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "20f60b7f48c22da556fc9946de9d70b0");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "c64d46135879de1362adc7a68cf5c97d");

        session = session.withChunkNumber(500);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "2e111f50c9509b5b5e1e922a06bc3386");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "328e566ab8dc5ab912a1bd2ab5514ed1");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(3000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "c06effcde07491152098ed35d07d3a2d");

        session = session.withChunkNumber(500);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "2aa846f9a7bc07ae0a72a0459869b2bb");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "a846903ce12ce47c25f4bf23c99f92ad");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "332ef3e0fe55292d6ab2bf7b883fac3e");

        session = session.withChunkNumber(5000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "d88797ef8c67b6d5e85480447b3fb41d");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "142a69503ce5268b1716752c64b20d68");
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "86390f038d2d15612f56fccc3d6d2a59");

        session = session.withChunkNumber(5000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "ea27c1d5e592fe0c235de311ee106ae5");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "80f52d7957b15a03a346d3cabb9248d4");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "648ee9a27cb0d6d1ea9dc6903d58fde1");

        session = session.withChunkNumber(50000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "a8e5900072a7bce16581e6d9b73d5e20");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "779de6438a2e1cd8d70ae9ccc62fd203");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15);
        ChunkBoundaries chunkBoundaries = splitWork(CUSTOMER, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CUSTOMER, session, "64aa5c9deada6149660aed5275b5a99d");
    }
}
