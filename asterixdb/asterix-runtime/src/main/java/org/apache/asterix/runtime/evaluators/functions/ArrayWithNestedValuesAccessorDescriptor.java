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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * array-with-nested-values-accessor(inputList, nestedValueAccessorFunction) returns a new list with the nested
 * values stored in inputList.
 *
 * This is a private function and should only be used by the compiler.
 */
public class ArrayWithNestedValuesAccessorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 4707476263212404438L;
    private AbstractCollectionType listType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayWithNestedValuesAccessorDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.FOREACH;
    }

    @Override
    public void setImmutableStates(Object... states) {
        final IAType type = (IAType) states[0];
        listType = type.getTypeTag() == ATypeTag.ANY ? DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE
                : (AbstractCollectionType) type;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayWithNestedValuesAccessorEval(args, ctx);
            }
        };
    }

    private class ArrayWithNestedValuesAccessorEval implements IScalarEvaluator {
        private final IScalarEvaluator listEval;
        private final IScalarEvaluator nestedValuesBuilderEval;

        private final VoidPointable listValue;
        private final VoidPointable nestedValue;
        private final SingleFieldFrameTupleReference itemValue;
        private final IAsterixListBuilder outputListBuilder;
        private final ListAccessor accessor;
        private final ArrayBackedValueStorage tempStorage;
        private final boolean itemSelfDescribing;

        public ArrayWithNestedValuesAccessorEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx)
                throws HyracksDataException {
            outputListBuilder = new OrderedListBuilder();
            listEval = args[0].createScalarEvaluator(ctx);
            nestedValuesBuilderEval = args[1].createScalarEvaluator(ctx);

            accessor = new ListAccessor();
            tempStorage = new ArrayBackedValueStorage();
            listValue = new VoidPointable();
            itemValue = new SingleFieldFrameTupleReference();
            itemSelfDescribing = listType.getItemType().getTypeTag() == ATypeTag.ANY;
            nestedValue = new VoidPointable();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            listEval.evaluate(tuple, listValue);

            if (PointableHelper.checkAndSetMissingOrNull(result, listValue)) {
                return;
            }

            final byte[] serArray = listValue.getByteArray();
            final int offset = listValue.getStartOffset();

            if (serArray[offset] != ATypeTag.ARRAY.serialize()) {
                throw new TypeMismatchException(sourceLoc, serArray[offset], ATypeTag.ARRAY.serialize());
            }

            accessor.reset(serArray, offset);
            outputListBuilder.reset(listType);
            for (int i = 0; i < accessor.size(); i++) {
                setItemValue(serArray, i);
                nestedValuesBuilderEval.evaluate(itemValue, nestedValue);
                outputListBuilder.addItem(nestedValue);
            }

            tempStorage.reset();
            outputListBuilder.write(tempStorage.getDataOutput(), true);
            result.set(tempStorage);
        }

        private void setItemValue(byte[] serArray, int index) throws HyracksDataException {
            final int itemOffset = accessor.getItemOffset(index);
            final int itemLength = accessor.getItemLength(itemOffset);
            if (itemSelfDescribing) {
                itemValue.reset(serArray, itemOffset, itemLength);
            } else {
                final DataOutput out = tempStorage.getDataOutput();
                tempStorage.reset();
                try {
                    out.writeByte(accessor.getItemType(itemOffset).serialize());
                    out.write(serArray, itemOffset, itemLength);
                    //+1 for type tag
                    itemValue.reset(tempStorage.getByteArray(), 0, itemLength + 1);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }
        }
    }
}
