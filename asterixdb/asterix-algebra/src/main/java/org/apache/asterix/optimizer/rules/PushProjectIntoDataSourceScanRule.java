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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushProjectIntoDataSourceScanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        final AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getInputs().isEmpty()) {
            return false;
        }
        final AbstractLogicalOperator project = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        if (project.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            return false;
        }
        final AbstractLogicalOperator inputOp = (AbstractLogicalOperator) project.getInputs().get(0).getValue();
        if (inputOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN
                && inputOp.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
            return false;
        }
        final AbstractScanOperator scan = (AbstractScanOperator) inputOp;
        if (scan.getProjectExpressions() == null) {
            return false;
        }

        final ProjectOperator projectOp = (ProjectOperator) project;
        if (scan.addProjectVariables(projectOp.getVariables())) {
            //Only remove project when it's not needed
             op.getInputs().set(0, project.getInputs().get(0));
        }
        context.computeAndSetTypeEnvironmentForOperator(scan);
        return true;
    }
}
