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
import static io.trino.tpcds.Table.STORE_RETURNS;

public class TestStoreReturnsGenerator
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(STORE_RETURNS);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(STORE_RETURNS), STORE_RETURNS, session, "2d6e049368329a08b9775f810fcbb210");
    }

    @Test
    public void testScaleFactor1()
    {
        Session session = TEST_SESSION.withScale(1);
        assertPartialMD5(1, session.getScaling().getRowCount(STORE_RETURNS), STORE_RETURNS, session, "0bd723c027e3ff03b457546190537889");
    }

    @Test
    public void testScaleFactor10()
    {
        Session session = TEST_SESSION.withScale(10).withParallelism(100).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "d1a1dd726e6eb6f4aa3399750f3d4d9e");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "e577e937e442976d8986569b4eb49f5f");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "e6742bd3e72f15bb9fccfb7d45cff1db");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "ef7a940df31e1f55844b9e9a2afd92cb");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "55a51fc39cdb9169ead48343655963f5");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "1ca848ac8ef50e0cb8b2899603594382");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(3000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "e9cc463345a4effccdddf3a1a5929a25");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "aace06546cd842ebfc28320876dcecbe");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "39db98597791db6a8b0f58c3c9b56c76");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "ac079ee6e87f8d7691c76134e5fb9a86");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "966f2355ca8c3c5fcf6d3663934cc9f9");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "02c0bad4b1e760580f7d86b92f84d4b9");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "7bfaa55268fb2aae826cef7f78ffa68e");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "e461cf51de3568a8af9325acf3e3b334");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "ed0235441ee5c58eea0b58b969cc36f0");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "7eec64ea250c49b71d7a5b3ccb32d689");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "02e3be17c61427e2f5d20c00ee0daee5");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "2fc53c654fbfec96b00459457a996b24");
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(300000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "c33cd68890b53b2fa61a318ac67340aa");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "1272ccf5c3858603850328544bd455fe");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "2455b4499f7f840e290b1c9676c98924");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(1000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "72c2b0f767a5eb2c2c514dc56c8a56d9");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "21108e1ccf189cb6d3540075532bc943");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "36f4ec14ea5b2613a967499108eda043");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15).withParallelism(150).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "8129ee13dbbbcb80b74336f160398cb4");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "0bf877361ec7a381482ccca458e5000a");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(STORE_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), STORE_RETURNS, session, "abb283fab1ace8d19c6aa2062087ea17");
    }
}
