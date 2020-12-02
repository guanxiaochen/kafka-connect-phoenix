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

package io.kafka.connect.phoenix.config;

import com.google.common.base.Preconditions;
import io.kafka.connect.phoenix.parser.TableInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;

import java.sql.Types;
import java.util.Map;

/**
 * @author Dhananjay
 */
public class PhoenixSinkConfig extends AbstractConfig {

    public static final String PQS_URL = "pqs.url";

    public static final String PHOENIX_TABLE_NAME = "phoenix.table";
    public static final String PHOENIX_TABLE_FIELDS = "phoenix.fields";

    public PhoenixSinkConfig(Map<String, String> originals) {
        this(CONFIG, originals);
    }

    public PhoenixSinkConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }

    /**
     * @param propertyName propertyName
     * @param defaultValue defaultValue
     * @return propertyValue
     */
    public String getPropertyValue(final String propertyName, final String defaultValue) {
        String propertyValue = getPropertyValue(propertyName);
        return propertyValue != null ? propertyValue : defaultValue;
    }

    /**
     * @param propertyName propertyName
     * @return propertyValue
     */
    public String getPropertyValue(final String propertyName) {
        Preconditions.checkNotNull(propertyName);
        return super.getString(propertyName);
    }

    /**
     * table info
     */
    public TableInfo getTableInfo() {
        return TableInfo.parse(getPropertyValue(PHOENIX_TABLE_NAME), getPropertyValue(PHOENIX_TABLE_FIELDS));
    }


    public static ConfigDef CONFIG = getConfigDef();
    private static ConfigDef getConfigDef() {
        ConfigDef def = new ConfigDef();
        def.define(PQS_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Phoenix Query Server url http://host:8765 of the hbase cluster");
        def.define(SinkConnectorConfig.TOPICS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "List of topics to consume, separated by commas");
        def.define(PHOENIX_TABLE_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "phoenix table name");
        def.define(PHOENIX_TABLE_FIELDS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "phoenix table fields");
        return def;
    }
}
