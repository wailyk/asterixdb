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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushFieldAccessToDataScan implements IAlgebraicRewriteRule {
    //Datasets payload variables
    private final List<LogicalVariable> recordVariables = new ArrayList<>();
    //Unnest produced-source variables map
    private final Map<LogicalVariable, LogicalVariable> unnestVariables = new HashMap<>();
    //Dataset scan operators
    private final List<AbstractScanOperator> scanOps = new ArrayList<>();
    //Expressions that have been pushed to dataset scan operator
    private final Map<LogicalVariable, AbstractScanOperator> pushedExpers = new HashMap<>();
    //Current scan operator that the expression will be pushed to
    private AbstractScanOperator changedScanOp;
    //Current operator that contains the expression to be pushed
    private ILogicalOperator op;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalOperator currentOp = opRef.getValue();
        final LogicalOperatorTag currentOpTag = currentOp.getOperatorTag();
        if (!context.getPhysicalOptimizationConfig().getExpressionPushdowns()) {
            return false;
        }

        if (currentOpTag == LogicalOperatorTag.DATASOURCESCAN || currentOpTag == LogicalOperatorTag.UNNEST_MAP) {
            setDataset(currentOp, (MetadataProvider) context.getMetadataProvider());
        } else if (currentOpTag == LogicalOperatorTag.UNNEST) {
            setUnnest(currentOp);
        } else if (currentOpTag == LogicalOperatorTag.SUBPLAN) {
            final SubplanOperator subplan = (SubplanOperator) currentOp;
            Mutable<ILogicalOperator> subOpRef = subplan.getNestedPlans().get(0).getRoots().get(0);
            ILogicalOperator subOp = subOpRef.getValue();
            rewritePre(subOpRef, context);
            while (subOp.getInputs() != null && !subOp.getInputs().isEmpty()) {
                subOpRef = subOp.getInputs().get(0);
                subOp = subOpRef.getValue();
                rewritePre(subOpRef, context);
            }
        }

        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (!context.getPhysicalOptimizationConfig().getExpressionPushdowns()) {
            return false;
        }

        op = opRef.getValue();
        if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
            final SubplanOperator subplan = (SubplanOperator) op;
            boolean changed = false;
            Mutable<ILogicalOperator> subOpRef = subplan.getNestedPlans().get(0).getRoots().get(0);
            ILogicalOperator subOp = subOpRef.getValue();
            changed |= rewritePost(subOpRef, context);
            while (subOp.getInputs() != null && !subOp.getInputs().isEmpty()) {
                subOpRef = subOp.getInputs().get(0);
                subOp = subOpRef.getValue();
                changed |= rewritePost(subOpRef, context);
            }
            return changed;
        } else if (op.getOperatorTag() != LogicalOperatorTag.SELECT
                && op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }

        final boolean changed;
        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            final SelectOperator selectOp = (SelectOperator) op;
            changed = pushFieldAccessExpression(selectOp.getCondition(), context);
        } else {
            final AssignOperator assignOp = (AssignOperator) op;
            changed = pushFieldAccessExpression(assignOp.getExpressions(), context);

        }

        if (changed) {
            changedScanOp.computeInputTypeEnvironment(context);
            op.computeInputTypeEnvironment(context);
            context.computeAndSetTypeEnvironmentForOperator(changedScanOp);
            context.computeAndSetTypeEnvironmentForOperator(op);
        }
        return changed;
    }

    private boolean pushFieldAccessExpression(List<Mutable<ILogicalExpression>> exprList, IOptimizationContext context)
            throws AlgebricksException {

        boolean changed = false;
        for (Mutable<ILogicalExpression> exprRef : exprList) {
            changed |= pushFieldAccessExpression(exprRef, context);
        }
        return changed;
    }

    private boolean pushFieldAccessExpression(Mutable<ILogicalExpression> exprRef, IOptimizationContext context)
            throws AlgebricksException {
        final ILogicalExpression expr = exprRef.getValue();
        final boolean changed;
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        final AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        //Get root expression input variable in case it is nested field access
        final LogicalVariable funcRootInputVar = getRootExpressionInputVariable(funcExpr);
        //Check if it's a field access
        if (funcRootInputVar != null) {
            final int recordVarIndex = recordVariables.indexOf(funcRootInputVar);
            //Is funcRootInputVar originated from a data-scan operator?
            if (recordVarIndex >= 0) {
                changedScanOp = scanOps.get(recordVarIndex);
                pushExpressionToScan(exprRef, context);
                changed = true;
            } else if (unnestVariables.containsKey(funcRootInputVar)) {
                wrapWithAccessor(funcRootInputVar, unnestVariables.get(funcRootInputVar), funcExpr, exprRef, context);
                changed = true;
            } else {
                changed = false;
            }
        } else {
            //Descend to the arguments expressions to see if any can be pushed
            changed = pushFieldAccessExpression(funcExpr.getArguments(), context);
        }

        return changed;
    }

    //TODO account for the case if the pushed expression has been referenced more than once
    private void wrapWithAccessor(LogicalVariable unnestProduced, LogicalVariable unnestSource,
            AbstractFunctionCallExpression funcExpr, Mutable<ILogicalExpression> exprRef, IOptimizationContext context)
            throws AlgebricksException {
        changedScanOp = pushedExpers.get(unnestSource);
        if (changedScanOp == null) {
            return;
        }

        final int scanExprVarIndex = changedScanOp.getVariables().indexOf(unnestSource);
        final Mutable<ILogicalExpression> scanExprRef = changedScanOp.getProjectExpressions().get(scanExprVarIndex);

        final AbstractFunctionCallExpression foreachFunc =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FOREACH));
        final AbstractFunctionCallExpression columnAccessorExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.FOREACH_ITEM_ACCESSOR));
        substituteRootExpression(funcExpr, columnAccessorExpr);
        foreachFunc.getArguments().add(new MutableObject<>(scanExprRef.getValue()));
        foreachFunc.getArguments().add(new MutableObject<>(funcExpr));
        exprRef.setValue(new VariableReferenceExpression(unnestProduced));
        scanExprRef.setValue(foreachFunc);

        changedScanOp.computeInputTypeEnvironment(context);
    }

    private void pushExpressionToScan(Mutable<ILogicalExpression> exprRef, IOptimizationContext context)
            throws AlgebricksException {
        //Add the corresponding variable to the scan operator
        final LogicalVariable newVar;
        final ILogicalExpression expr = exprRef.getValue();

        //Compute the output type of the expression
        IVariableTypeEnvironment env = op.computeOutputTypeEnvironment(context);
        IAType type = (IAType) env.getType(expr);

        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            //Substitute the condition expression with new variable reference expression
            newVar = context.newVar();
            final VariableReferenceExpression varExpr = new VariableReferenceExpression(newVar);
            exprRef.setValue(varExpr);
        } else {
            //It's assign. Use the same assigned variable to avoid substituting for parent operators
            final AssignOperator assignOp = (AssignOperator) op;
            final int exprIndex = assignOp.getExpressions().indexOf(exprRef);

            if (exprIndex >= 0) {
                /* 
                 * Hack alert!!!
                 * Change the produced variable of the AssignOperator to a new variable.
                 * The optimizer will eliminate the AssignOperator as its produced variable are not used. 
                 */
                newVar = assignOp.getVariables().get(exprIndex);
                assignOp.getExpressions().get(exprIndex).setValue(new VariableReferenceExpression(newVar));
                assignOp.getVariables().set(exprIndex, context.newVar());
            } else {
                //Substitute the INNER expression with new variable reference expression
                newVar = context.newVar();
                final VariableReferenceExpression varExpr = new VariableReferenceExpression(newVar);
                exprRef.setValue(varExpr);
            }
        }

        //Add fieldAccessExpr to the scan
        changedScanOp.addProjectExpression(new MutableObject<>(expr), type);
        changedScanOp.getVariables().add(newVar);
        pushedExpers.put(newVar, changedScanOp);
    }

    private void setDataset(ILogicalOperator op, MetadataProvider mp) throws AlgebricksException {
        final AbstractScanOperator scan = (AbstractScanOperator) op;
        final DataSource dataSource = getDatasourceInfo(mp, scan);

        if (dataSource == null) {
            return;
        }
        final String dataverse = dataSource.getId().getDataverseName();
        final String datasetName = dataSource.getId().getDatasourceName();
        final Dataset dataset = mp.findDataset(dataverse, datasetName);

        if (dataset != null && dataset.getDatasetType() == DatasetType.INTERNAL) {
            DatasetDataSource datasetDataSource = (DatasetDataSource) dataSource;
            final LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(scan.getVariables());
            scan.addPayloadExpression(new VariableReferenceExpression(recordVar));
            recordVariables.add(recordVar);
            scanOps.add(scan);
        }
    }

    private void setUnnest(ILogicalOperator op) {
        final UnnestOperator unnest = (UnnestOperator) op;
        final LogicalVariable unnestVar = unnest.getVariables().get(0);
        final ILogicalExpression expr = unnest.getExpressionRef().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) unnest.getExpressionRef().getValue();
        final LogicalVariable sourceVar = getRootExpressionInputVariable(funcExpr);
        unnestVariables.put(unnestVar, sourceVar);
    }

    private static LogicalVariable getRootExpressionInputVariable(AbstractFunctionCallExpression funcExpr) {
        ILogicalExpression currentExpr = funcExpr.getArguments().get(0).getValue();
        while (currentExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            currentExpr = ((AbstractFunctionCallExpression) currentExpr).getArguments().get(0).getValue();
        }

        if (currentExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return ((VariableReferenceExpression) currentExpr).getVariableReference();
        }
        return null;
    }

    private static void substituteRootExpression(AbstractFunctionCallExpression funcExpr, ILogicalExpression newExpr) {
        Mutable<ILogicalExpression> ref = funcExpr.getArguments().get(0);
        ILogicalExpression currentExpr = ref.getValue();
        while (currentExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            ref = ((AbstractFunctionCallExpression) currentExpr).getArguments().get(0);
            currentExpr = ref.getValue();
        }

        ref.setValue(newExpr);
    }

    private static DataSource getDatasourceInfo(MetadataProvider metadataProvider, AbstractScanOperator scanOp)
            throws AlgebricksException {
        final String dataverse;
        final String dataset;
        final DataSource dataSource;

        if (scanOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            final DataSourceScanOperator scan = (DataSourceScanOperator) scanOp;
            dataSource = (DataSource) scan.getDataSource();
        } else {
            final UnnestMapOperator unnest = (UnnestMapOperator) scanOp;
            final AbstractFunctionCallExpression funcExpr =
                    (AbstractFunctionCallExpression) unnest.getExpressionRef().getValue();
            dataverse = ConstantExpressionUtil.getStringArgument(funcExpr, 2);
            dataset = ConstantExpressionUtil.getStringArgument(funcExpr, 3);
            if (!ConstantExpressionUtil.getStringArgument(funcExpr, 0).equals(dataset)) {
                return null;
            }

            dataSource = metadataProvider.findDataSource(new DataSourceId(dataverse, dataset));
        }

        return dataSource;
    }
}
