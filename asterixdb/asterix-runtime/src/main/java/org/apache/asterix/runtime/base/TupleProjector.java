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
package org.apache.asterix.runtime.base;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleProjector;

public class TupleProjector implements ITupleProjector {
    private final IScalarEvaluator[] fieldAccessEvals;
    private final IPointable resultPointable;

    public TupleProjector(IScalarEvaluator[] fieldAccessEvals) {
        this.fieldAccessEvals = fieldAccessEvals;
        resultPointable = new VoidPointable();
    }

    @Override
    public IValueReference projectField(IFrameTupleReference tuple, int index) throws HyracksDataException {
        fieldAccessEvals[index].evaluate(tuple, resultPointable);
        return resultPointable;
    }

    @Override
    public int getNumberOfFields() {
        return fieldAccessEvals.length;
    }
}
