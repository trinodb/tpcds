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

import com.google.common.collect.ImmutableList;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.util.List;

@Command(name = "dsdgen", description = "data generator for TPC-DS", mixinStandardHelpOptions = true)
public class Driver
        implements Runnable
{
    @Mixin
    public Options options = new Options();

    public static void main(String[] args)
    {
        System.exit(new CommandLine(new Driver()).execute(args));
    }

    @Override
    public void run()
    {
        Session session = options.toSession();
        List<Table> tablesToGenerate;
        if (session.generateOnlyOneTable()) {
            tablesToGenerate = ImmutableList.of(session.getOnlyTableToGenerate());
        }
        else {
            tablesToGenerate = Table.getBaseTables();
        }

        for (int i = 1; i <= session.getParallelism(); i++) {
            int chunkNumber = i;
            new Thread(() -> {
                TableGenerator tableGenerator = new TableGenerator(session.withChunkNumber(chunkNumber));
                tablesToGenerate.forEach(tableGenerator::generateTable);
            }).start();
        }
    }
}
