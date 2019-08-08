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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;

import java.io.IOException;

public class AsterixTupleFilter implements ITupleFilter {
    private final IBinaryBooleanInspector boolInspector;
    private final IScalarEvaluator eval;
    private final IPointable p = VoidPointable.FACTORY.createPointable();
    private int quantifier = -1;
    private int itemIndex;
    private boolean metUnknown = false;
    private final ListAccessor listAccessor = new ListAccessor();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    public AsterixTupleFilter(IHyracksTaskContext ctx, IScalarEvaluatorFactory evalFactory,
            IBinaryBooleanInspector boolInspector, int quantifier) throws HyracksDataException {
        this.boolInspector = boolInspector;
        this.eval = evalFactory.createScalarEvaluator(ctx);
        this.quantifier = quantifier;
    }

    @Override
    public boolean accept(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, p);
        if (quantifier == -1) {
            return boolInspector.getBooleanValue(p.getByteArray(), p.getStartOffset(), p.getLength());
        } else {
            init(p);
            IPointable cur = VoidPointable.FACTORY.createPointable();
            boolean res;
            do {
                if (!step(cur)) {
                    break;
                }
                res = boolInspector.getBooleanValue(cur.getByteArray(), cur.getStartOffset(), cur.getLength());
                // if the quantifier is some
                if (quantifier == 0 && res) {
                   return true;
                }
                // if the quantifier is every
                if (quantifier == 1 && !res) {
                    return false;
                }

            } while (true);

            if (quantifier == 0) {
                // there is no true result
                return false;
            } else {
                // there is no false result
                return true;
            }
        }

    }

    public void init(IPointable p) throws HyracksDataException {
        metUnknown = false;
        byte typeTag = p.getByteArray()[p.getStartOffset()];
        if (typeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                || typeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            metUnknown = true;
            return;
        }
        if (typeTag != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                && typeTag != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
            return;
        }
        listAccessor.reset(p.getByteArray(), p.getStartOffset());
        itemIndex = 0;
    }

    public boolean step(IPointable result) throws HyracksDataException {
        try {
            if (!metUnknown) {
                if (itemIndex < listAccessor.size()) {
                    resultStorage.reset();
                    listAccessor.writeItem(itemIndex, resultStorage.getDataOutput());
                    result.set(resultStorage);
                    ++itemIndex;
                    return true;
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return false;
    }

}
