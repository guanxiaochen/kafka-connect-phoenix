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


package io.kafka.connect.phoenix.sink;

import io.kafka.connect.phoenix.PhoenixClient;
import io.kafka.connect.phoenix.PhoenixConnectionManager;
import io.kafka.connect.phoenix.config.PhoenixSinkConfig;
import io.kafka.connect.phoenix.parser.EventParsingException;
import io.kafka.connect.phoenix.parser.TableInfo;
import io.kafka.connect.phoenix.util.ToPhoenixRecordFunction;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixSinkTask extends SinkTask {
	
	private static final Logger log =LoggerFactory.getLogger(PhoenixSinkTask.class);

    private TableInfo tableInfo;
    private ToPhoenixRecordFunction toPhoenixRecordFunction;

	private PhoenixClient phoenixClient;
	
    @Override
    public String version() {
        return PhoenixSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        PhoenixSinkConfig sinkConfig = new PhoenixSinkConfig(props);
        this.toPhoenixRecordFunction = new ToPhoenixRecordFunction(sinkConfig);
        this.phoenixClient = new PhoenixClient(new PhoenixConnectionManager(sinkConfig.getPropertyValue(PhoenixSinkConfig.PQS_URL)));
        this.tableInfo = sinkConfig.getTableInfo();
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
    	long startTime = System.nanoTime();
    	try{
    	    if(this.tableInfo.getFields().isEmpty()) {
                loadSchemaFields(records.iterator().next());
            }
            List<Map<String, Object>> phoenixRecords = records.stream().map(r -> toPhoenixRecordFunction.apply(r)).collect(toList());
            this.phoenixClient.execute(tableInfo, phoenixRecords);
        } catch (Exception e) {
        	log.error("Exception while persisting records"+ records, e);
        }
      log.info("Time taken to persist "+ records.size() +" sink records in ms"+(System.nanoTime()-startTime)/1000);
    }

    private void loadSchemaFields(SinkRecord record) {
        Schema schema = record.valueSchema();
        if (schema == null) {
            throw new EventParsingException("valueSchema not found, or config for " + PhoenixSinkConfig.PHOENIX_TABLE_FIELDS);
        }
        for (Field field : schema.fields()) {
            this.tableInfo.addFields(field.name(), field.schema().type());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // NO-OP
    }

    @Override
    public void stop() {
        // NO-OP
    }

}