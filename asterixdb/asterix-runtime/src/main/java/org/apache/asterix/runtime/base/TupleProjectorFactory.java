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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITupleProjector;
import org.apache.hyracks.storage.am.common.api.ITupleProjectorFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TupleProjectorFactory implements ITupleProjectorFactory {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final IScalarEvaluatorFactory[] evalFactories;
    private final RecordDescriptor inputRecordDescriptor;
    private final String projectedFields;

    public TupleProjectorFactory(IScalarEvaluatorFactory[] fieldAccessEvalFactories,
            RecordDescriptor inputRecordDescriptor, ILogicalExpression[] projectedExpressions,
            ILogicalExpression payloadExpression) throws CompilationException {
        this.evalFactories = fieldAccessEvalFactories;
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.projectedFields = getProjectedFields(projectedExpressions, payloadExpression).toString();
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

    /**
     * Return Json representation of the requested fields
     */
    @Override
    public String getProjectionProperties() {
        return projectedFields;
    }

    private static JsonNode getProjectedFields(ILogicalExpression[] projectedExpressions,
            ILogicalExpression payloadExpression) throws CompilationException {
        ObjectNode rootNode = OBJECT_MAPPER.createObjectNode();
        for (ILogicalExpression expr : projectedExpressions) {
            //null child represent the leaf nodes of the requested schema
            convertToType(expr, rootNode, payloadExpression, null);
        }
        return rootNode;
    }

    private static void convertToType(ILogicalExpression expr, ObjectNode rootNode,
            ILogicalExpression payloadExpression, JsonNode childType) throws CompilationException {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(funcExpr.getFunctionIdentifier())) {
                ILogicalExpression objectExpr = funcExpr.getArguments().get(0).getValue();
                if (isPayload(objectExpr, payloadExpression)) {
                    rootNode.set(ConstantExpressionUtil.getStringArgument(funcExpr, 1), childType);
                } else {
                    final ObjectNode node = OBJECT_MAPPER.createObjectNode();
                    convertToType(objectExpr, rootNode, payloadExpression, node);
                    node.set(ConstantExpressionUtil.getStringArgument(funcExpr, 1), childType);
                }
            }
        }

    }

    private static boolean isPayload(ILogicalExpression expr, ILogicalExpression payloadExpression) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE && payloadExpression.equals(expr);
    }
}
