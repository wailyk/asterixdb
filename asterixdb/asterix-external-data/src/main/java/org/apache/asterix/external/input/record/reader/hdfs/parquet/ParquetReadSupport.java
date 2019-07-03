/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ParquetReadSupport extends ReadSupport<IValueReference> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public ReadContext init(InitContext context) {
        //TODO get the required fields
        final String requestedSchemaString = context.getConfiguration().get(ExternalDataConstants.KEY_REQUESTED_FIELDS);
        MessageType requestedSchema = getRequestedSchema(requestedSchemaString, context.getFileSchema());
        return new ReadContext(requestedSchema, Collections.emptyMap());
    }

    @Override
    public RecordMaterializer<IValueReference> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {

        final ADMRecordMaterializer materializer;
        try {
            materializer = new ADMRecordMaterializer(readContext);
        } catch (HyracksDataException e) {
            throw new RuntimeException(e);
        }
        return materializer;
    }

    private class ADMRecordMaterializer extends RecordMaterializer<IValueReference> {
        private final RootConverter rootConverter;

        public ADMRecordMaterializer(ReadContext readContext) throws HyracksDataException {
            rootConverter = new RootConverter(readContext.getRequestedSchema());
        }

        @Override
        public IValueReference getCurrentRecord() {
            return rootConverter.getRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }

    }

    private static MessageType getRequestedSchema(String requestedSchemaString, MessageType fileSchema) {
        if ("*".equals(requestedSchemaString)) {
            return fileSchema;
        }

        try {
            ObjectNode root = (ObjectNode) OBJECT_MAPPER.readTree(requestedSchemaString);
            MessageTypeBuilder mBuilder = Types.buildMessage();
            Iterator<String> iter = root.fieldNames();
            while (iter.hasNext()) {
                mBuilder.addField(fileSchema.getType(iter.next()));
            }
            return mBuilder.named("asterix");
        } catch (

        IOException e) {
            throw new RuntimeException(e);
        }
    }

}
