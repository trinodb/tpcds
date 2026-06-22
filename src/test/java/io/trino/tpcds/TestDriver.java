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

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static io.trino.tpcds.Table.CALL_CENTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDriver
{
    private static Driver parse(String... args)
    {
        Driver driver = new Driver();
        new CommandLine(driver).parseArgs(args);
        return driver;
    }

    @Test
    public void testParsing()
    {
        Driver driver = parse("--scale", "10", "--suffix", "abcd");
        assertThat(driver.options.scale).isEqualTo(10.0);
        assertThat(driver.options.suffix).isEqualTo("abcd");
        assertThat(driver.options.directory).isEqualTo(".");
        Session session = driver.options.toSession();
        assertThat(session.generateOnlyOneTable()).isFalse();
    }

    @Test
    public void testParsingTableName()
    {
        Driver driver = parse("--table", "call_center");
        assertThat(driver.options.table).isEqualTo("call_center");
        Session session = driver.options.toSession();
        assertThat(session.generateOnlyOneTable()).isTrue();
        assertThat(session.getOnlyTableToGenerate()).isEqualTo(CALL_CENTER);
    }

    @Test
    public void testInvalidTable()
    {
        Driver driver = parse("--table", "bad_table_name");
        assertThat(driver.options.table).isEqualTo("bad_table_name");
        assertThatThrownBy(driver.options::toSession)
                .isInstanceOf(InvalidOptionException.class)
                .hasMessage("Invalid value for table: 'bad_table_name'. ");
    }

    @Test
    public void testBadDirectory()
    {
        Driver driver = parse("--directory", "");
        assertThat(driver.options.table).isNull();
        assertThatThrownBy(driver.options::toSession)
                .isInstanceOf(InvalidOptionException.class)
                .hasMessage("Invalid value for directory: ''. Directory cannot be an empty string");
    }

    @Test
    public void testBadSuffix()
    {
        Driver driver = parse("--suffix", "");
        assertThat(driver.options.table).isNull();
        assertThatThrownBy(driver.options::toSession)
                .isInstanceOf(InvalidOptionException.class)
                .hasMessage("Invalid value for suffix: ''. Suffix cannot be an empty string");
    }

    @Test
    public void testInvalidScale()
    {
        Driver driver = parse("--scale", "-1");
        assertThatThrownBy(driver.options::toSession)
                .isInstanceOf(InvalidOptionException.class)
                .hasMessage("Invalid value for scale: '-1.0'. Scale must be greater than 0 and less than 100000");
    }

    @Test
    public void testDecimalScale()
    {
        Driver driver = parse("--scale", "0.01");
        assertThat(driver.options.scale).isEqualTo(0.01);
    }
}
