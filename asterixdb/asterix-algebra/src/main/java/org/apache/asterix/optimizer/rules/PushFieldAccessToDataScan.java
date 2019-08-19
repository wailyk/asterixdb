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
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.expressions.*;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushFieldAccessToDataScan implements IAlgebraicRewriteRule {
    //Datasets payload variables
    private final List<LogicalVariable> recordVariables = new ArrayList<>();
    //Unnest produced-source variables map
    private final Map<LogicalVariable, LogicalVariable> unnestVariables = new HashMap<>();
    //Dataset scan operators
    private final List<AbstractScanOperator> scanOps = new ArrayList<>();
    // project operators
    private final List<ProjectOperator> projectOps = new ArrayList<>();
    // assign operaotors
    private final List<AssignOperator> assignOps = new ArrayList<>();
    // select operator to remove if subplan is removed
    private Mutable<ILogicalOperator> selectOpToBeRemoved;
    // aggregate variable
    private LogicalVariable aggregateVariable;
    //Expressions that have been pushed to dataset scan operator
    private final Map<LogicalVariable, AbstractScanOperator> pushedExpers = new HashMap<>();
    //Current scan operator that the expression will be pushed to
    private AbstractScanOperator changedScanOp;
    //Current operator that contains the expression to be pushed
    private ILogicalOperator op;
    // If the current operator is select
    private boolean isSelect;
    // 0: some/any 1: every
    private int quantifier = -1;

    // subplan pattern to remove
    private LogicalOperatorTag[] pattern = { LogicalOperatorTag.AGGREGATE, LogicalOperatorTag.SELECT,
            LogicalOperatorTag.UNNEST, LogicalOperatorTag.NESTEDTUPLESOURCE };
    private boolean removeSubplan = false;

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
        } else if (currentOpTag == LogicalOperatorTag.ASSIGN) {
            assignOps.add((AssignOperator) currentOp);
        } else if (currentOpTag == LogicalOperatorTag.PROJECT) {
            projectOps.add((ProjectOperator) currentOp);
        } else if (currentOpTag == LogicalOperatorTag.SUBPLAN) {
            final SubplanOperator subplan = (SubplanOperator) currentOp;
            Mutable<ILogicalOperator> subOpRef = subplan.getNestedPlans().get(0).getRoots().get(0);
            ILogicalOperator subOp = subOpRef.getValue();
            if (subOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                AggregateOperator aggregateOperator = (AggregateOperator) subOp;
                aggregateVariable = aggregateOperator.getVariables().get(0);
                if (aggregateOperator.getExpressions().get(0).getValue().toString().equals("empty-stream()")) {
                    quantifier = 1; // every
                } else {
                    quantifier = 0; // some
                }
            }
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

            // check if the subplan should be removed
            removeSubplan = true;
            checkRemoveSubplanPattern(subOp, 0);

            changed |= rewritePost(subOpRef, context);
            LogicalVariable aggregateVariable = null;
            while (subOp.getInputs() != null && !subOp.getInputs().isEmpty()) {
                subOpRef = subOp.getInputs().get(0);
                subOp = subOpRef.getValue();
                changed |= rewritePost(subOpRef, context);
            }
            if (removeSubplan) {
                opRef.setValue(opRef.getValue().getInputs().get(0).getValue());

            }
            return changed;
        } else if (op.getOperatorTag() != LogicalOperatorTag.SELECT
                && op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }

        final boolean changed;

        if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            final SelectOperator selectOp = (SelectOperator) op;
            isSelect = true;
            changed = pushFieldAccessExpression(selectOp.getCondition(), context);
            if (selectOp.getCondition().getValue().toString().equals(aggregateVariable.toString())) {
                opRef.setValue(opRef.getValue().getInputs().get(0).getValue());
            }
        } else {
            final AssignOperator assignOp = (AssignOperator) op;
            isSelect = false;
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
        // if it is select, remove the unused unnest source variable
        if (isSelect) {
            DataSourceScanOperator dataSourceScanOperator = (DataSourceScanOperator) changedScanOp;
            dataSourceScanOperator.setSelectCondition(scanExprRef);
            dataSourceScanOperator.setQuantifier(quantifier);
            changedScanOp.getProjectExpressions().remove(scanExprVarIndex);
            changedScanOp.getVariables().remove(scanExprVarIndex);
            // remove the unnest source in assign operators
            for (AssignOperator assignOperator : assignOps) {
                final int assignExprVarIndex = assignOperator.getExpressions().indexOf(unnestSource);
                if (assignExprVarIndex >= 0) {
                    assignOperator.getExpressions().remove(assignExprVarIndex);
                    assignOperator.getVariables().remove(assignExprVarIndex);
                }
            }
            // remove the unnest source in project operators
            for (ProjectOperator projectOperator : projectOps) {
                final int projectExprVarIndex = projectOperator.getVariables().indexOf(unnestSource);
                if (projectExprVarIndex >= 0) {
                    projectOperator.getVariables().remove(projectExprVarIndex);
                }
            }

        }
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

    private void checkRemoveSubplanPattern(ILogicalOperator op, int i) {
        /**
         * if there is pattern
         * aggregate <== select <== unnest <== nested tuple source
         * Then push down the filter and remove the subplan
         */
        if (op.getOperatorTag() != pattern[i]) {
            removeSubplan = false;
            return;
        }
        if (i > 3 || !op.getInputs().isEmpty()) {
            return;
        }
        checkRemoveSubplanPattern(op.getInputs().get(0).getValue(), ++i);
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
