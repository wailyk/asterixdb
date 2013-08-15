/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression;

import java.io.StringReader;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.feeds.BuiltinFeedPolicies;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class ConnectFeedStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier datasetName;
    private final String feedName;
    private final String policy;
    private Query query;
    private int varCounter;
    private boolean forceConnect = false;

    public static final String WAIT_FOR_COMPLETION = "wait-for-completion-feed";

    public ConnectFeedStatement(Pair<Identifier, Identifier> feedNameCmp, Pair<Identifier, Identifier> datasetNameCmp,
            String policy, int varCounter) {
        if (feedNameCmp.first != null && datasetNameCmp.first != null
                && !feedNameCmp.first.getValue().equals(datasetNameCmp.first.getValue())) {
            throw new IllegalArgumentException("Dataverse for source feed and target dataset do not match");
        }
        this.dataverseName = feedNameCmp.first != null ? feedNameCmp.first
                : datasetNameCmp.first != null ? datasetNameCmp.first : null;
        this.datasetName = datasetNameCmp.second;
        this.feedName = feedNameCmp.second.getValue();
        this.policy = policy != null ? policy : BuiltinFeedPolicies.DEFAULT_POLICY.getPolicyName();
        this.varCounter = varCounter;
    }

    public ConnectFeedStatement(Identifier dataverseName, Identifier feedName, Identifier datasetName, String policy,
            int varCounter) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.feedName = feedName.getValue();
        this.policy = policy != null ? policy : BuiltinFeedPolicies.DEFAULT_POLICY.getPolicyName();
        this.varCounter = varCounter;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx, Dataset targetDataset, Feed sourceFeed)
            throws MetadataException {
    	   query = new Query();
           FunctionSignature appliedFunction = sourceFeed.getAppliedFunction();
           Function function = null;
           String adapterOutputType = null;
           if (appliedFunction != null) {
               function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, appliedFunction);
               if (function == null) {
                   throw new MetadataException(" Unknown function " + function);
               } else if (function.getParams().size() > 1) {
                   throw new MetadataException(" Incompatible function: " + appliedFunction
                           + " Number if arguments must be 1");
               }
           }

           org.apache.commons.lang3.tuple.Pair<IAdapterFactory, ARecordType> factoryOutput = null;
           try {
               factoryOutput = FeedUtil.getFeedFactoryAndOutput(sourceFeed, mdTxnCtx);
               adapterOutputType = factoryOutput.getRight().getTypeName();
           } catch (AlgebricksException ae) {
               throw new MetadataException(ae);
           }

           StringBuilder builder = new StringBuilder();
           builder.append("set" + " " + FunctionUtils.IMPORT_PRIVATE_FUNCTIONS + " " + "'" + Boolean.TRUE + "'" + ";\n");
           builder.append("insert into dataset " + datasetName + " ");

           if (appliedFunction == null) {
               builder.append(" (" + " for $x in feed-ingest ('" + feedName + "'" + "," + "'" + adapterOutputType + "'"
                       + "," + "'" + targetDataset.getDatasetName() + "'" + ")");
               builder.append(" return $x");
           } else {
               if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                   String param = function.getParams().get(0);
                   builder.append(" (" + " for" + " " + param + " in feed-ingest ('" + feedName + "'" + "," + "'"
                           + adapterOutputType + "'" + "," + "'" + targetDataset.getDatasetName() + "'" + ")");
                   builder.append(" let $y:=(" + function.getFunctionBody() + ")" + " return $y");
               } else {
                   builder.append(" (" + " for $x in feed-ingest ('" + feedName + "'" + "," + "'" + adapterOutputType
                           + "'" + "," + "'" + targetDataset.getDatasetName() + "'" + ")");
                   builder.append(" let $y:=" + sourceFeed.getDataverseName() + "." + function.getName() + "(" + "$x"
                           + ")");
                   builder.append(" return $y");
               }

           }
           builder.append(")");
           builder.append(";");
           AQLParser parser = new AQLParser(new StringReader(builder.toString()));

           List<Statement> statements;
           try {
               statements = parser.Statement();
               query = ((InsertStatement) statements.get(1)).getQuery();
           } catch (ParseException pe) {
               throw new MetadataException(pe);
           }

    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.CONNECT_FEED;
    }

    public String getPolicy() {
        return policy;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitConnectFeedStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public boolean forceConnect() {
        return forceConnect;
    }

    public void setForceConnect(boolean forceConnect) {
        this.forceConnect = forceConnect;
    }

    public String getFeedName() {
        return feedName;
    }

}
