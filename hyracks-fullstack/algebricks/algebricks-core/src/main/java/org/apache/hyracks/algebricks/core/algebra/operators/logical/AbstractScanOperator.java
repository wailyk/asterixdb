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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;

public abstract class AbstractScanOperator extends AbstractLogicalOperator {
    protected List<LogicalVariable> variables;
    protected boolean projectPushed = false;
    protected List<Mutable<ILogicalExpression>> projectExpressions;
    protected List<Object> projectExpressionTypes;
    protected ILogicalExpression payloadExpression;
    protected final List<LogicalVariable> projectVars;
    protected final int orginalNumOfVars;

    public AbstractScanOperator(List<LogicalVariable> variables) {
        this.variables = variables;
        this.orginalNumOfVars = variables.size();
        projectVars = new ArrayList<>();
    }

    public List<LogicalVariable> getVariables() {
        return variables;
    }

    public List<LogicalVariable> getScanVariables() {
        return variables;
    }

    public void setVariables(List<LogicalVariable> variables) {
        this.variables = variables;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
        schema.addAll(variables);
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                for (LogicalVariable v : variables) {
                    target.addVariable(v);
                }
            }
        };
    }

    public boolean addProjectVariables(Collection<LogicalVariable> vars) {
        projectVars.addAll(variables);
        projectVars.retainAll(vars);
        //Remove project if there are no propagated variables from child operators
//        final boolean removeProject = projectVars.size() == vars.size();
//        final boolean[] indexesToRetain = new boolean[variables.size()];
//        for (int i = 0; i < projectVars.size(); i++) {
//            final int indexToRetain = variables.indexOf(projectVars.get(i));
//            if (indexToRetain >= 0) {
//                indexesToRetain[indexToRetain] = true;
//            }
//        }
//
//        for (int i = indexesToRetain.length - 1; i >= 0; i--) {
//            if (!indexesToRetain[i]) {
//                projectExpressions.remove(i);
//                projectExpressionTypes.remove(i);
//                if (i >= orginalNumOfVars) {
//                    //Remove added variable for projectedExpression
//                    variables.remove(i);
//                }
//            }
//        }
        projectPushed = true;
        return false;
    }

    public List<LogicalVariable> getProjectVariables() {
        return projectVars;
    }

    public List<Mutable<ILogicalExpression>> getProjectExpressions() {
        return projectExpressions;
    }

    public List<Object> getProjectExpressionsTypes() {
        return projectExpressionTypes;
    }

    /**
     * To add a project expression, it has to have its type computed
     *
     * @param projectExpression
     * @param type
     */
    public void addProjectExpression(Mutable<ILogicalExpression> projectExpression, Object type) {
        if (this.projectExpressions == null) {
            this.projectExpressions = new ArrayList<>();
            projectExpressionTypes = new ArrayList<>();

            for (int i = 0; i < variables.size(); i++) {
                final MutableObject<ILogicalExpression> varExpr =
                        new MutableObject<>(new VariableReferenceExpression(variables.get(i)));
                projectExpressions.add(varExpr);
                projectExpressionTypes.add(getVariableType(i));
            }
        }

        this.projectExpressions.add(projectExpression);
        this.projectExpressionTypes.add(type);
    }

    protected abstract Object getVariableType(int i);

    public void addPayloadExpression(ILogicalExpression payloadExpression) {
        this.payloadExpression = payloadExpression;
    }

    public ILogicalExpression getPayloadExpression() {
        return payloadExpression;
    }

    public boolean isProjectPushed() {
        return projectPushed;
    }
}
