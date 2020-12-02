/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kafka.connect.phoenix;

import io.kafka.connect.phoenix.parser.TableInfo;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dhananjay
 */
public class PhoenixClient {


    private static final Logger log = LoggerFactory.getLogger(PhoenixClient.class);

    /**
     *
     */
    private final PhoenixConnectionManager connectionManager;

    private static final int COMMIT_INTERVAL = 100;

    /**
     * @param connectionManager
     */
    public PhoenixClient(final PhoenixConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }


    /**
     * @param tableInfo table
     * @return
     */
    private String formUpsert(final TableInfo tableInfo) {

        StringBuilder query_part1 = new StringBuilder();
        StringBuilder query_part2 = new StringBuilder();
        tableInfo.getFields().forEach(f -> {
            query_part1.append(",\"").append(f.getName().toUpperCase()).append("\"");
            query_part2.append(",?");
        });
        String sql = String.format("upsert into %s(%s) values (%s)", tableInfo.getFullName(), query_part1.substring(1), query_part2.substring(1));
        log.info("Query formed " + sql);
        return sql;
    }


    public void execute(final TableInfo tableInfo, List<Map<String, Object>> records) {
        String sql = formUpsert(tableInfo);
        try (Connection connection = this.connectionManager.getConnection(); PreparedStatement ps = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            AtomicInteger rowCounter = new AtomicInteger(0);
            records.forEach(r -> {
                int paramIndex = 1;
                try {
                    //Iterate over fields
                    List<TableInfo.Field> fields = tableInfo.getFields();
                    for (TableInfo.Field f : fields) {
                        Object value = r.get(f.getName());
                        if (value == null) {
                            ps.setNull(paramIndex++, f.getType());
                            continue;
                        }

                        //log.error("field "+f.name() +" Going for value "+String.valueOf(value));
                        switch (f.getType()) {
                            case Types.VARCHAR: {
                                ps.setString(paramIndex++, MapUtils.getString(r, f.getName()));
                            }
                            break;
                            case Types.BOOLEAN: {
                                ps.setBoolean(paramIndex++, MapUtils.getBoolean(r, f.getName()));
                            }
                            break;
                            case Types.BINARY: {
                                ps.setBytes(paramIndex++, MapUtils.getString(r, f.getName()).getBytes());
                            }
                            break;
                            case Types.FLOAT: {
                                ps.setFloat(paramIndex++, MapUtils.getFloat(r, f.getName()));
                            }
                            break;
                            case Types.DOUBLE: {
                                ps.setDouble(paramIndex++, MapUtils.getDouble(r, f.getName()));
                            }
                            break;
                            case Types.TINYINT: {
                                ps.setByte(paramIndex++, MapUtils.getByte(r, f.getName()));
                            }
                            break;
                            case Types.SMALLINT: {
                                ps.setInt(paramIndex++, MapUtils.getShort(r, f.getName()));
                            }
                            break;
                            case Types.INTEGER: {
                                ps.setInt(paramIndex++, MapUtils.getInteger(r, f.getName()));
                            }
                            break;
                            case Types.BIGINT: {
                                ps.setLong(paramIndex++, MapUtils.getLong(r, f.getName()));
                            }
                            break;
                        }
                    }
                    ps.executeUpdate();

                    if (rowCounter.incrementAndGet() % COMMIT_INTERVAL == 0) {
                        connection.commit();
                        rowCounter.set(0);
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
