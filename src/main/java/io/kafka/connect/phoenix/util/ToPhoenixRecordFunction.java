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

package io.kafka.connect.phoenix.util;

import java.util.Map;
import java.util.Set;

import io.kafka.connect.phoenix.parser.PhoenixRecordParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import io.kafka.connect.phoenix.config.PhoenixSinkConfig;
import io.kafka.connect.phoenix.parser.EventParser;

/**
 * 
 * @author Dhananjay
 *
 */
public class ToPhoenixRecordFunction implements Function<SinkRecord, Map<String, Object>>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(ToPhoenixRecordFunction.class);
    
	private final PhoenixSinkConfig sinkConfig;
    
    private final EventParser eventParser;
    
    public ToPhoenixRecordFunction(final PhoenixSinkConfig sinkConfig){
    	this.sinkConfig = sinkConfig;
    	this.eventParser = new PhoenixRecordParser();
    }
	
	@Override
	public Map<String,Object> apply(SinkRecord sinkRecord) {
		try {
			Preconditions.checkNotNull(sinkRecord);
			return this.eventParser.parseValueObject(sinkRecord);
		} catch (Exception e) {
			LOGGER.error("Exception while parsing sink record from topic " + sinkRecord.topic() + " key " + sinkRecord.key(),e);
			throw new RuntimeException(e);
		}	
	}

    /**
     * Returns the name space based table for given topic name.
     * This derives name space based on the member partition of the sink record received.
     * 
     *
     */
    public String tableName(final String topic) {
        return sinkConfig.getPropertyValue(String.format(PhoenixSinkConfig.HBASE_TABLE_NAME, topic)).toUpperCase();
    }

	public EventParser getEventParser() {
		return eventParser;
	}
}
