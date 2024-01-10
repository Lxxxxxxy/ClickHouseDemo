package org.example;

import java.nio.file.Paths;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;

/**
 * @author Lxxxxxxy_
 * @date 2024/01/09 18:36
 */
public class ClickHouseClientDemo {
    public static void main(String[] args) throws ClickHouseException {
        // 此处的http和下方的ClickHouseProtocol.HTTP相对应
        ClickHouseNodes servers = ClickHouseNodes
            .of("http://server:port/test" + "?load_balancing_policy=random&health_check_interval=5000&failover=2");
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
            ClickHouseResponse response = client.read(servers).write()
                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .query(
                    "insert into user_info select c1, c2, c3, c4 from input('c1 String, c2 UInt8, c3 UInt8, c4 UInt8')")
                .data(Paths.get("demo.csv").toString()).executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            summary.getWrittenRows();
        }
        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
            ClickHouseResponse response = client.read(servers).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                .query("select * from user_info").executeAndWait()) {
            for (ClickHouseRecord r : response.records()) {
                System.out.println(r.getValue(0).asString() + "," + r.getValue(1).asInteger() + ","
                    + r.getValue(2).asInteger() + "," + r.getValue(3).asInteger());
            }
        }
    }
}
