package io.kafka.connect.phoenix;

import io.kafka.connect.phoenix.parser.TableInfo;
import junit.framework.TestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PhoenixClientTest extends TestCase {

    /**
     * create table presto_test.type_test(
     *     "ROW" VARCHAR: primary key,
     *     DATA.v_string: VARCHAR,
     *     DATA.v_int: INTEGER,
     *     DATA.v_uint: INTEGER,
     *     DATA.v_bigint: BIGINT,
     *     DATA.v_ulong: BIGINT,
     *     DATA.v_tinyint: TINYINT,
     *     DATA.v_utinyint: TINYINT,
     *     DATA.v_smallint: SMALLINT,
     *     DATA.v_usmallint: SMALLINT,
     *     DATA.v_float: FLOAT,
     *     DATA.v_ufloat: FLOAT,
     *     DATA.v_double: DOUBLE,
     *     DATA.v_udouble: DOUBLE,
     *     DATA.v_decimal: DOUBLE,
     *     DATA.v_boolean: BOOLEAN,
     *     DATA.v_binary: BINARY
     *     ) column_encoded_bytes=0;
     */
    public void testExecute() {

        PhoenixClient phoenixClient = new PhoenixClient(new PhoenixConnectionManager("http://localhost:8765"));
        String fullName = "my_test.type_test";
        String fields = "ROW: VARCHAR, v_string: VARCHAR, v_int: INTEGER, v_uint: INTEGER, v_bigint: BIGINT, v_ulong: BIGINT, v_tinyint: TINYINT, v_utinyint: TINYINT, v_smallint: SMALLINT, v_usmallint: SMALLINT, v_float: FLOAT, v_ufloat: FLOAT, v_double: DOUBLE, v_udouble: DOUBLE, v_decimal: DOUBLE, v_boolean: BOOLEAN, v_binary: BINARY";
        TableInfo parse = TableInfo.parse(fullName, fields);

        Map<String, Object> record = new HashMap<>();
        record.put("ROW", "202010001");
        record.put("v_string", "202010201");
        record.put("v_int", "-1");
        record.put("v_uint", "1");
        record.put("v_bigint", "-2");
        record.put("v_ulong", "2");
        record.put("v_tinyint", "-3");
        record.put("v_utinyint", "3");
        record.put("v_smallint", "-4");
        record.put("v_usmallint", "4");
        record.put("v_float", "-5.5");
        record.put("v_ufloat", "5.5");
        record.put("v_double", "-6.6");
        record.put("v_udouble", "6.6");
        record.put("v_decimal", "-7.7");
        record.put("v_boolean", "true");
        record.put("v_binary", "bccsadd");
        phoenixClient.execute(parse, Collections.singletonList(record));

    }
}