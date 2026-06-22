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
import static io.trino.tpcds.Table.WEB_RETURNS;

public class WebReturnsGeneratorTest
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(WEB_RETURNS);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(WEB_RETURNS), WEB_RETURNS, session, "03113cf0514e70768a1ef3269233d88f");
    }

    @Test
    public void testScaleFactor1()
    {
        Session session = TEST_SESSION.withScale(1);
        assertPartialMD5(1, session.getScaling().getRowCount(WEB_RETURNS), WEB_RETURNS, session, "4c413aabff1f06cc23a0d61db4a1df3c");
    }

    @Test
    public void testScaleFactor10()
    {
        // Test the whole table because the C generator has a bug where
        // if you tell it to generate a chunk, it won't generate anything.
        Session session = TEST_SESSION.withScale(10);
        assertPartialMD5(1, session.getScaling().getRowCount(WEB_RETURNS), WEB_RETURNS, session, "247dc93969b021f02b9909cc75140ec9");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "b172351b65f17806290dbd1cb2d3831a");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "4267deb40e3ae1449a7fdc756bc2b0a9");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "10550f5c19e1650033f27d9ef54a0a23");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(3000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "0c7abe57014bbfb9a48f4d941fe2dd32");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "51170cfb70a79b60e5cc3d4a5c7bfc9a");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "834be760dcb7b1668620bca341d50286");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "dee2bcfd74f6f855b82c8544fe7638f8");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "bfa4a3c51b18d63987e5235a65975c54");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "596cc26fed60960a0fd6735daee6860b");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "7dbc6417f084a557b7d65a99dba1c740");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "e5e6be3b1b66157be7e91de24eaa2cdc");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "6c985baf3a4c8c8fc2c2761168c394c8");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "a4fdd9aa601a5978da09a4ad0726c915");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "96d1ecbf0163ed955f37926f67808f54");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "d5fcda1ec661686a0afa248d69fc41b3");
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(300000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "d45532fe87e37f446da96db4aed33e42");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "5fbb137ffaca4075247afaf9b0bdb24b");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "e485ffd538211e39b43701e1038e003a");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(1000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "93a120ce488b62ae533cb42f0fae8415");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "543be56650506577bf6c98046121e82d");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(WEB_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), WEB_RETURNS, session, "046a5de598005d3b9536e96aa718e735");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15);
        assertPartialMD5(1, 10000, WEB_RETURNS, session, "8b19d294d3d046ff0b0051993a13f873");
    }
}
