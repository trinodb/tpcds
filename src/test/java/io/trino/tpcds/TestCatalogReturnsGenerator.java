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
import static io.trino.tpcds.Table.CATALOG_RETURNS;

public class TestCatalogReturnsGenerator
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(CATALOG_RETURNS);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(CATALOG_RETURNS), CATALOG_RETURNS, session, "87f5ba60ec430157eaf268b1bfebfd1c");
    }

    @Test
    public void testScaleFactor1()
    {
        Session session = TEST_SESSION.withScale(1);
        assertPartialMD5(1, session.getScaling().getRowCount(CATALOG_RETURNS), CATALOG_RETURNS, session, "f7cc7c90d74f59cbf73c36383eaf62cc");
    }

    @Test
    public void testScaleFactor10()
    {
        Session session = TEST_SESSION.withScale(10).withParallelism(100).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "700e7b1cda566bd7a296004ce4a44386");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "e73ac5b8dd6c883df9c4534a0b01ab92");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "d10591eecab71a689d61fd0cd6527c61");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "56512ed0f5e48c4fd0385c9d1fbba2d3");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "8b26b093164ff103a30055b4bcd3d9bb");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "eb1f66924f1882bd9975c2ab4bc76f93");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "22cbdfa0396693cceccda6b1dfb73f88");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "d581405600d497638294bb8a24a638e3");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "b2bc323f19448bef42b6a92e02c2ca01");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "4f37b38530aa13d41668fbfd0e283448");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "a43f7d0a5bf231a42a63d38dcc6b4591");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "86be226065ab8120092aea86b62d3217");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "86dd6b2b8a5abcb2d65fa93dabe7a039");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "e7b719a57432bf2438b2f53d850283fb");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "e4e1a9b3d5e53c991e0dc4ed872d268d");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "af33f8220fb429f497a668d7ed8c5fe7");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "4cb0e672872e72f65d1cf82c4ca06065");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "27db4b50d6f956942a5599d78caf646a");
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(300000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "b0a8f04d3b787f158d6881b2a7c7d59b");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "c028997e2468c26cfeb2ed6a606e868c");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "4d9b13bcc30c743f34991221ee7766ee");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(1000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "f6b25db057dfd6905d59a2e015d9efd1");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "995284b4c4d649bdfd06160d30995a3a");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "1940a4bb734bfee98059d204d8121fb6");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15).withParallelism(150).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "f8e0dd9a234c5b519591ceaddd729d02");

        session = session.withChunkNumber(10);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "c8fac7acffaba560e0393ebbd02cc461");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(CATALOG_RETURNS, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), CATALOG_RETURNS, session, "af320196814d49d63dd3609ed5eb8dd0");
    }
}
