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

import java.io.DataOutput;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.schema.GroupType;

public class RootConverter extends ObjectConverter {
    private final ArrayBackedValueStorage rootBuffer;

    public RootConverter(GroupType parquetType) throws HyracksDataException {
        super(null, parquetType, new ParserContext());
        this.rootBuffer = new ArrayBackedValueStorage();
    }

    @Override
    protected DataOutput getParentDataOutput() {
        rootBuffer.reset();
        return rootBuffer.getDataOutput();
    }

    public IValueReference getRecord() {
        return rootBuffer;
    }

}
