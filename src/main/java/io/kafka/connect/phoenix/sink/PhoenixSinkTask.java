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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kafka.connect.phoenix.PhoenixClient;
import io.kafka.connect.phoenix.PhoenixConnectionManager;
import io.kafka.connect.phoenix.config.PhoenixSinkConfig;
import io.kafka.connect.phoenix.util.ToPhoenixRecordFunction;

/**
 * 
 * @author Dhananjay
 *
 */
public class PhoenixSinkTask extends SinkTask {
	
	private static final Logger log =LoggerFactory.getLogger(PhoenixSinkTask.class);

    private ToPhoenixRecordFunction toPhoenixRecordFunction;
	
	private PhoenixClient phoenixClient;
	
    @Override
    public String version() {
        return PhoenixSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        final PhoenixSinkConfig sinkConfig = new PhoenixSinkConfig(props);

        this.toPhoenixRecordFunction = new ToPhoenixRecordFunction(sinkConfig);
        this.phoenixClient = new PhoenixClient(new PhoenixConnectionManager(sinkConfig.getPropertyValue(PhoenixSinkConfig.PQS_URL)));
    }

    @Override
    public void put(final Collection<SinkRecord> records) { 
    	long startTime = System.nanoTime();
    	try{
			Map<PhoenixSchemaInfo, List<SinkRecord>> bySchema = records.stream().collect(
					groupingBy(e -> new PhoenixSchemaInfo(toPhoenixRecordFunction.tableName(e.topic()), e.valueSchema())));
			bySchema.forEach((key, value) -> {
				List<Map<String, Object>> phoenixRecords = value.stream().map(r -> toPhoenixRecordFunction.apply(r)).collect(toList());
				this.phoenixClient.execute(key.getTableName(), key.getSchema(), phoenixRecords);
			});
        }catch(Exception e){
        	log.error("Exception while persisting records"+ records,e);
        }
      log.info("Time taken to persist "+ records.size() +" sink records in ms"+(System.nanoTime()-startTime)/1000);
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