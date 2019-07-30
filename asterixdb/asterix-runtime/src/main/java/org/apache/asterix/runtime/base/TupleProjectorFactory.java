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
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITupleProjector;
import org.apache.hyracks.storage.am.common.api.ITupleProjectorFactory;

public class TupleProjectorFactory implements ITupleProjectorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory[] evalFactories;
    private final RecordDescriptor inputRecordDescriptor;

    public TupleProjectorFactory(IScalarEvaluatorFactory[] fieldAccessEvalFactories,
            RecordDescriptor inputRecordDescriptor) {
        this.evalFactories = fieldAccessEvalFactories;
        this.inputRecordDescriptor = inputRecordDescriptor;
    }

    @Override
    public RecordDescriptor getInputRecordDescriptor() {
        return inputRecordDescriptor;
    }

    @Override
    public ITupleProjector createTupleProjector(IHyracksTaskContext context) throws HyracksDataException {
        final IScalarEvaluator[] evals = new IScalarEvaluator[evalFactories.length];
        for (int i = 0; i < evalFactories.length; i++) {
            evals[i] = evalFactories[i].createScalarEvaluator(context);
        }
        return new TupleProjector(evals);
    }

    public int getFieldNum() {
        return evalFactories.length;
    }

}
