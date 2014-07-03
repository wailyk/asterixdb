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

package edu.uci.ics.asterix.metadata.declared;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixStorageProperties;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixVirtualBufferCacheProvider;
import edu.uci.ics.asterix.common.context.ITransactionSubsystemProvider;
import edu.uci.ics.asterix.common.context.TransactionSubsystemProvider;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.AsterixLSMTreeInsertDeleteOperatorDescriptor;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager.ResourceType;
import edu.uci.ics.asterix.common.transactions.JobId;
import edu.uci.ics.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import edu.uci.ics.asterix.formats.base.IDataFormat;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.dataset.hints.DatasetHints.DatasetCardinalityHint;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.ExternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.ExternalFile;
import edu.uci.ics.asterix.metadata.entities.Feed;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.external.IndexingConstants;
import edu.uci.ics.asterix.metadata.feeds.BuiltinFeedPolicies;
import edu.uci.ics.asterix.metadata.feeds.EndFeedMessage;
import edu.uci.ics.asterix.metadata.feeds.ExternalDataScanOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedMessageOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory.SupportedOperation;
import edu.uci.ics.asterix.metadata.feeds.IFeedMessage;
import edu.uci.ics.asterix.metadata.feeds.IGenericAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.metadata.utils.ExternalDatasetsRegistry;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.runtime.base.AsterixTupleFilterFactory;
import edu.uci.ics.asterix.runtime.external.ExternalBTreeSearchOperatorDescriptor;
import edu.uci.ics.asterix.runtime.external.ExternalRTreeSearchOperatorDescriptor;
import edu.uci.ics.asterix.runtime.formats.FormatUtils;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.asterix.runtime.job.listener.JobEventListenerFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexModificationOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.opcallbacks.PrimaryIndexSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexModificationOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import edu.uci.ics.asterix.transaction.management.opcallbacks.SecondaryIndexSearchOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.AsterixRuntimeComponentsProvider;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IResultSerializerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.SinkWriterRuntimeFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IResultSerializerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.PartitionedLSMInvertedIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.HilbertDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveDoubleComparatorFactory;
import edu.uci.ics.hyracks.storage.am.rtree.linearize.ZCurveIntComparatorFactory;

public class AqlMetadataProvider implements IMetadataProvider<AqlSourceId, String> {
    private static Logger LOGGER = Logger.getLogger(AqlMetadataProvider.class.getName());
    private MetadataTransactionContext mdTxnCtx;
    private boolean isWriteTransaction;
    private Map<String, String[]> stores;
    private Map<String, String> config;
    private IAWriterFactory writerFactory;
    private FileSplit outputFile;
    private boolean asyncResults;
    private ResultSetId resultSetId;
    private IResultSerializerFactoryProvider resultSerializerFactoryProvider;

    private final Dataverse defaultDataverse;
    private JobId jobId;
    private Map<String, Integer> locks;

    private final AsterixStorageProperties storageProperties;

    public static final Map<String, String> adapterFactoryMapping = initializeAdapterFactoryMapping();

    public String getPropertyValue(String propertyName) {
        return config.get(propertyName);
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Map<String, String[]> getAllStores() {
        return stores;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public AqlMetadataProvider(Dataverse defaultDataverse) {
        this.defaultDataverse = defaultDataverse;
        this.stores = AsterixAppContextInfo.getInstance().getMetadataProperties().getStores();
        this.storageProperties = AsterixAppContextInfo.getInstance().getStorageProperties();
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public Dataverse getDefaultDataverse() {
        return defaultDataverse;
    }

    public String getDefaultDataverseName() {
        return defaultDataverse == null ? null : defaultDataverse.getDataverseName();
    }

    public void setWriteTransaction(boolean writeTransaction) {
        this.isWriteTransaction = writeTransaction;
    }

    public void setWriterFactory(IAWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    public void setMetadataTxnContext(MetadataTransactionContext mdTxnCtx) {
        this.mdTxnCtx = mdTxnCtx;
    }

    public MetadataTransactionContext getMetadataTxnContext() {
        return mdTxnCtx;
    }

    public IAWriterFactory getWriterFactory() {
        return this.writerFactory;
    }

    public FileSplit getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(FileSplit outputFile) {
        this.outputFile = outputFile;
    }

    public boolean getResultAsyncMode() {
        return asyncResults;
    }

    public void setResultAsyncMode(boolean asyncResults) {
        this.asyncResults = asyncResults;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    public void setResultSetId(ResultSetId resultSetId) {
        this.resultSetId = resultSetId;
    }

    public void setResultSerializerFactoryProvider(IResultSerializerFactoryProvider rafp) {
        this.resultSerializerFactoryProvider = rafp;
    }

    public IResultSerializerFactoryProvider getResultSerializerFactoryProvider() {
        return resultSerializerFactoryProvider;
    }

    @Override
    public AqlDataSource findDataSource(AqlSourceId id) throws AlgebricksException {
        AqlSourceId aqlId = (AqlSourceId) id;
        try {
            return lookupSourceInMetadata(aqlId);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public boolean isWriteTransaction() {
        return isWriteTransaction;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getScannerRuntime(
            IDataSource<AqlSourceId> dataSource, List<LogicalVariable> scanVariables,
            List<LogicalVariable> projectVariables, boolean projectPushed, IOperatorSchema opSchema,
            IVariableTypeEnvironment typeEnv, JobGenContext context, JobSpecification jobSpec, Object implConfig)
            throws AlgebricksException {
        try {
            switch (((AqlDataSource) dataSource).getDatasourceType()) {
                case FEED:
                    return buildFeedIntakeRuntime(jobSpec, dataSource);
                case INTERNAL_DATASET:
                    return buildInternalDatasetScan(jobSpec, scanVariables, opSchema, typeEnv, dataSource, context,
                            implConfig);

                case EXTERNAL_DATASET:
                    return buildExternalDatasetScan(jobSpec, dataSource);

                default:
                    throw new IllegalArgumentException();

            }
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInternalDatasetScan(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            IDataSource<AqlSourceId> dataSource, JobGenContext context, Object implConfig) throws AlgebricksException,
            MetadataException {
        AqlSourceId asid = dataSource.getId();
        String dataverseName = asid.getDataverseName();
        String datasetName = asid.getDatasetName();
        Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataverseName, datasetName, datasetName);
        return buildBtreeRuntime(jobSpec, outputVars, opSchema, typeEnv, context, true, false,
                ((DatasetDataSource) dataSource).getDataset(), primaryIndex.getIndexName(), null, null, true, true,
                implConfig);
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDatasetScan(JobSpecification jobSpec,
            IDataSource<AqlSourceId> dataSource) throws AlgebricksException, MetadataException {
        Dataset dataset = ((DatasetDataSource) dataSource).getDataset();
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                .getDatatype();
        return buildExternalDataScannerRuntime(jobSpec, itemType, (ExternalDatasetDetails) dataset.getDatasetDetails(),
                NonTaggedDataFormat.INSTANCE, dataset);

    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildExternalDataScannerRuntime(
            JobSpecification jobSpec, IAType itemType, ExternalDatasetDetails datasetDetails, IDataFormat format,
            Dataset dataset) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }

        IAdapterFactory adapterFactory;
        String adapterName;
        DatasourceAdapter adapterEntity;
        String adapterFactoryClassname;
        try {
            adapterName = datasetDetails.getAdapter();
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            if (adapterEntity != null) {
                adapterFactoryClassname = adapterEntity.getClassname();
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            } else {
                adapterFactoryClassname = adapterFactoryMapping.get(adapterName);
                if (adapterFactoryClassname == null) {
                    throw new AlgebricksException(" Unknown adapter :" + adapterName);
                }
                adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
            }

            Map<String, String> configuration = datasetDetails.getProperties();

            // check to see if dataset is indexed
            Index filesIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(),
                    dataset.getDatasetName().concat(IndexingConstants.EXTERNAL_FILE_INDEX_NAME_SUFFIX));

            if (filesIndex != null && filesIndex.getPendingOp() == 0) {
                // get files
                List<ExternalFile> files = MetadataManager.INSTANCE.getDatasetExternalFiles(mdTxnCtx, dataset);
                Iterator<ExternalFile> iterator = files.iterator();
                while (iterator.hasNext()) {
                    if (iterator.next().getPendingOp() != ExternalFilePendingOp.PENDING_NO_OP) {
                        iterator.remove();
                    }
                }
                ((IGenericAdapterFactory) adapterFactory).setFiles(files);
            }

            switch (adapterFactory.getAdapterType()) {
                case GENERIC:
                    ((IGenericAdapterFactory) adapterFactory).configure(configuration, (ARecordType) itemType);
                    break;
                case TYPED:
                    ((ITypedAdapterFactory) adapterFactory).configure(configuration);
                    break;
            }
        } catch (AlgebricksException ae) {
            throw ae;
        } catch (Exception e) {
            throw new AlgebricksException("Unable to create adapter " + e);
        }

        if (!(adapterFactory.getSupportedOperations().equals(SupportedOperation.READ) || adapterFactory
                .getSupportedOperations().equals(SupportedOperation.READ_WRITE))) {
            throw new AlgebricksException(" External dataset adapter does not support read operation");
        }

        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

        ExternalDataScanOperatorDescriptor dataScanner = new ExternalDataScanOperatorDescriptor(jobSpec, scannerDesc,
                adapterFactory);

        AlgebricksPartitionConstraint constraint;
        try {
            constraint = adapterFactory.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(dataScanner, constraint);
    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildScannerRuntime(JobSpecification jobSpec,
            IAType itemType, IParseFileSplitsDecl decl, IDataFormat format) throws AlgebricksException {
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Can only scan datasets of records.");
        }
        ARecordType rt = (ARecordType) itemType;
        ITupleParserFactory tupleParser = format.createTupleParser(rt, decl);
        FileSplit[] splits = decl.getSplits();
        IFileSplitProvider scannerSplitProvider = new ConstantFileSplitProvider(splits);
        ISerializerDeserializer payloadSerde = format.getSerdeProvider().getSerializerDeserializer(itemType);
        RecordDescriptor scannerDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });
        IOperatorDescriptor scanner = new FileScanOperatorDescriptor(jobSpec, scannerSplitProvider, tupleParser,
                scannerDesc);
        String[] locs = new String[splits.length];
        for (int i = 0; i < splits.length; i++) {
            locs[i] = splits[i].getNodeName();
        }
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(locs);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(scanner, apc);
    }

    @SuppressWarnings("rawtypes")
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildFeedIntakeRuntime(JobSpecification jobSpec,
            IDataSource<AqlSourceId> dataSource) throws AlgebricksException {

        FeedDataSource feedDataSource = (FeedDataSource) dataSource;
        FeedIntakeOperatorDescriptor feedIngestor = null;
        Triple<IAdapterFactory, ARecordType, AdapterType> factoryOutput = null;
        AlgebricksPartitionConstraint constraint = null;

        try {
            factoryOutput = FeedUtil.getFeedFactoryAndOutput(feedDataSource.getFeed(), mdTxnCtx);
            IAdapterFactory adapterFactory = factoryOutput.first;
            ARecordType adapterOutputType = factoryOutput.second;
            AdapterType adapterType = factoryOutput.third;

            ISerializerDeserializer payloadSerde = NonTaggedDataFormat.INSTANCE.getSerdeProvider()
                    .getSerializerDeserializer(adapterOutputType);
            RecordDescriptor feedDesc = new RecordDescriptor(new ISerializerDeserializer[] { payloadSerde });

            FeedPolicy feedPolicy = (FeedPolicy) ((AqlDataSource) dataSource).getProperties().get(
                    BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY);
            if (feedPolicy == null) {
                throw new AlgebricksException("Feed not configured with a policy");
            }
            feedPolicy.getProperties().put(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY, feedPolicy.getPolicyName());
            switch (adapterType) {
                case INTERNAL:
                    feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, new FeedConnectionId(
                            feedDataSource.getDatasourceDataverse(), feedDataSource.getDatasourceName(), feedDataSource
                                    .getFeedConnectionId().getDatasetName()), adapterFactory,
                            (ARecordType) adapterOutputType, feedDesc, feedPolicy.getProperties());
                    break;
                case EXTERNAL:
                    String libraryName = feedDataSource.getFeed().getAdaptorName().split("#")[0];
                    feedIngestor = new FeedIntakeOperatorDescriptor(jobSpec, feedDataSource.getFeedConnectionId(),
                            libraryName, adapterFactory.getClass().getName(), feedDataSource.getFeed()
                                    .getAdaptorConfiguration(), (ARecordType) adapterOutputType, feedDesc,
                            feedPolicy.getProperties());
                    break;
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Cofigured feed intake operator with " + adapterType + " adapter");
            }
            constraint = factoryOutput.first.getPartitionConstraint();
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedIngestor, constraint);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildSendFeedMessageRuntime(
            JobSpecification jobSpec, String dataverse, String feedName, String dataset, IFeedMessage feedMessage,
            String[] locations) throws AlgebricksException {
        AlgebricksPartitionConstraint partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locations);
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, dataverse, feedName,
                dataset, feedMessage);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, partitionConstraint);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDisconnectFeedMessengerRuntime(
            JobSpecification jobSpec, String dataverse, String feedName, String dataset, FeedActivity feedActivity)
            throws AlgebricksException {
        List<String> feedLocations = new ArrayList<String>();
        String[] ingestLocs = feedActivity.getFeedActivityDetails().get(FeedActivityDetails.INGEST_LOCATIONS)
                .split(",");
        for (String loc : ingestLocs) {
            feedLocations.add(loc);
        }
        FeedConnectionId feedId = new FeedConnectionId(dataverse, feedName, dataset);
        String[] locations = feedLocations.toArray(new String[] {});
        IFeedMessage feedMessage = new EndFeedMessage(feedId);
        return buildSendFeedMessageRuntime(jobSpec, dataverse, feedName, dataset, feedMessage, locations);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildBtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainNull, Dataset dataset, String indexName,
            int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive,
            Object implConfig) throws AlgebricksException {
        boolean isSecondary = true;
        int numSecondaryKeys = 0;
        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            if (primaryIndex != null && dataset.getDatasetType() != DatasetType.EXTERNAL) {
                isSecondary = !indexName.equals(primaryIndex.getIndexName());
            }
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            int[] bloomFilterKeyFields;
            ITypeTraits[] typeTraits;
            IBinaryComparatorFactory[] comparatorFactories;
            if (isSecondary) {
                Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName);
                numSecondaryKeys = secondaryIndex.getKeyFieldNames().size();
                bloomFilterKeyFields = new int[numSecondaryKeys];
                for (int i = 0; i < numSecondaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
                typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, 0, outputVars.size(), typeEnv, context);
                comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(outputVars, 0,
                        outputVars.size(), typeEnv, context);
            } else {
                bloomFilterKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    bloomFilterKeyFields[i] = i;
                }
                String itemTypeName = dataset.getItemTypeName();
                ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                        dataset.getDataverseName(), itemTypeName).getDatatype();

                typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
                comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset, itemType,
                        context.getBinaryComparatorFactoryProvider());
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc;
            try {
                spPc = splitProviderAndPartitionConstraintsForDataset(dataset.getDataverseName(),
                        dataset.getDatasetName(), indexName);
            } catch (Exception e) {
                throw new AlgebricksException(e);
            }

            ISearchOperationCallbackFactory searchCallbackFactory = null;
            if (isSecondary) {
                searchCallbackFactory = new SecondaryIndexSearchOperationCallbackFactory();
            } else {
                JobId jobId = ((JobEventListenerFactory) jobSpec.getJobletEventListenerFactory()).getJobId();
                int datasetId = dataset.getDatasetId();
                int[] primaryKeyFields = new int[numPrimaryKeys];
                for (int i = 0; i < numPrimaryKeys; i++) {
                    primaryKeyFields[i] = i;
                }

                AqlMetadataImplConfig aqlMetadataImplConfig = (AqlMetadataImplConfig) implConfig;
                ITransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
                if (aqlMetadataImplConfig != null && aqlMetadataImplConfig.isInstantLock()) {
                    searchCallbackFactory = new PrimaryIndexInstantSearchOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);
                } else {
                    searchCallbackFactory = new PrimaryIndexSearchOperationCallbackFactory(jobId, datasetId,
                            primaryKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);
                }
            }
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixRuntimeComponentsProvider rtcProvider = AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER;
            BTreeSearchOperatorDescriptor btreeSearchOp;
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields, highKeyFields,
                        lowKeyInclusive, highKeyInclusive, new LSMBTreeDataflowHelperFactory(
                                new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                                compactionInfo.second, isSecondary ? new SecondaryIndexOperationTrackerProvider(
                                        dataset.getDatasetId()) : new PrimaryIndexOperationTrackerProvider(
                                        dataset.getDatasetId()), rtcProvider,
                                LSMBTreeIOOperationCallbackFactory.INSTANCE, getStorageProperties()
                                        .getBloomFilterFalsePositiveRate(), !isSecondary), retainInput, retainNull,
                        context.getNullWriterFactory(), searchCallbackFactory);
            } else {
                // External dataset <- use the btree with buddy btree->
                // Be Careful of Key Start Index ?
                int[] buddyBreeFields = new int[] { numSecondaryKeys };
                ExternalBTreeWithBuddyDataflowHelperFactory indexDataflowHelperFactory = new ExternalBTreeWithBuddyDataflowHelperFactory(
                        compactionInfo.first, compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                                dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE, getStorageProperties()
                                .getBloomFilterFalsePositiveRate(), buddyBreeFields,
                        ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this));
                btreeSearchOp = new ExternalBTreeSearchOperatorDescriptor(jobSpec, outputRecDesc, rtcProvider,
                        rtcProvider, spPc.first, typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields,
                        highKeyFields, lowKeyInclusive, highKeyInclusive, indexDataflowHelperFactory, retainInput,
                        retainNull, context.getNullWriterFactory(), searchCallbackFactory);
            }

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    /* BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
             appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(), spPc.first,
             typeTraits, comparatorFactories, bloomFilterKeyFields, lowKeyFields, highKeyFields,
             lowKeyInclusive, highKeyInclusive, new LSMBTreeDataflowHelperFactory(
                     new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                     compactionInfo.second, isSecondary ? new SecondaryIndexOperationTrackerProvider(
                             dataset.getDatasetId()) : new PrimaryIndexOperationTrackerProvider(
                             dataset.getDatasetId()), rtcProvider, LSMBTreeIOOperationCallbackFactory.INSTANCE,
                     storageProperties.getBloomFilterFalsePositiveRate(), !isSecondary), retainInput,
             retainNull, context.getNullWriterFactory(), searchCallbackFactory);

     return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeSearchOp, spPc.second);

    } catch (MetadataException me) {
     throw new AlgebricksException(me);
    }
    }*/

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildRtreeRuntime(JobSpecification jobSpec,
            List<LogicalVariable> outputVars, IOperatorSchema opSchema, IVariableTypeEnvironment typeEnv,
            JobGenContext context, boolean retainInput, boolean retainNull, Dataset dataset, String indexName,
            int[] keyFields) throws AlgebricksException {
        try {
            ARecordType recType = (ARecordType) findType(dataset.getDataverseName(), dataset.getItemTypeName());
            int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();

            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            if (secondaryIndex == null) {
                throw new AlgebricksException("Code generation error: no index " + indexName + " for dataset "
                        + dataset.getDatasetName());
            }
            List<String> secondaryKeyFields = secondaryIndex.getKeyFieldNames();
            int numSecondaryKeys = secondaryKeyFields.size();
            if (numSecondaryKeys != 1) {
                throw new AlgebricksException(
                        "Cannot use "
                                + numSecondaryKeys
                                + " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
            }
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(secondaryKeyFields.get(0), recType);
            IAType keyType = keyTypePair.first;
            if (keyType == null) {
                throw new AlgebricksException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
            }
            int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
            int numNestedSecondaryKeyFields = numDimensions * 2;
            IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
            for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }

            RecordDescriptor outputRecDesc = JobGenHelper.mkRecordDescriptor(typeEnv, opSchema, context);
            // IS NOT THE VARIABLE BELOW ALWAYS = 0 ??
            int keysStartIndex = outputRecDesc.getFieldCount() - numNestedSecondaryKeyFields - numPrimaryKeys;
            if (retainInput) {
                keysStartIndex -= numNestedSecondaryKeyFields;
            }
            IBinaryComparatorFactory[] comparatorFactories = JobGenHelper.variablesToAscBinaryComparatorFactories(
                    outputVars, keysStartIndex, numNestedSecondaryKeyFields, typeEnv, context);
            ITypeTraits[] typeTraits = JobGenHelper.variablesToTypeTraits(outputVars, keysStartIndex,
                    numNestedSecondaryKeyFields + numPrimaryKeys, typeEnv, context);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> spPc = splitProviderAndPartitionConstraintsForDataset(
                    dataset.getDataverseName(), dataset.getDatasetName(), indexName);

            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, context.getBinaryComparatorFactoryProvider());
            int[] btreeFields = new int[primaryComparatorFactories.length];
            for (int i = 0; i < btreeFields.length; i++) {
                btreeFields[i] = i + numSecondaryKeys;
            }

            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            ISearchOperationCallbackFactory searchCallbackFactory = new SecondaryIndexSearchOperationCallbackFactory();

            RTreeSearchOperatorDescriptor rtreeSearchOp;
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                rtreeSearchOp = new RTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, keyFields, new LSMRTreeDataflowHelperFactory(
                                valueProviderFactories, RTreePolicyType.RTREE, primaryComparatorFactories,
                                new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                                compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                                        dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                                LSMRTreeIOOperationCallbackFactory.INSTANCE, proposeLinearizer(
                                        nestedKeyType.getTypeTag(), comparatorFactories.length), getStorageProperties()
                                        .getBloomFilterFalsePositiveRate(), btreeFields), retainInput, retainNull,
                        context.getNullWriterFactory(), searchCallbackFactory);

            } else {
                // External Dataset
                ExternalRTreeDataflowHelperFactory indexDataflowHelperFactory = new ExternalRTreeDataflowHelperFactory(
                        valueProviderFactories, RTreePolicyType.RTREE,
                        IndexingConstants.getBuddyBtreeComparatorFactories(), compactionInfo.first,
                        compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                        proposeLinearizer(nestedKeyType.getTypeTag(), comparatorFactories.length),
                        getStorageProperties().getBloomFilterFalsePositiveRate(),
                        new int[] { numNestedSecondaryKeyFields },
                        ExternalDatasetsRegistry.INSTANCE.getAndLockDatasetVersion(dataset, this));
                // Create the operator
                rtreeSearchOp = new ExternalRTreeSearchOperatorDescriptor(jobSpec, outputRecDesc,
                        appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                        spPc.first, typeTraits, comparatorFactories, keyFields, indexDataflowHelperFactory,
                        retainInput, retainNull, context.getNullWriterFactory(), searchCallbackFactory);
            }
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeSearchOp, spPc.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    @Override
    public Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint> getWriteFileRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc) {
        FileSplitDataSink fsds = (FileSplitDataSink) sink;
        FileSplitSinkId fssi = (FileSplitSinkId) fsds.getId();
        FileSplit fs = fssi.getFileSplit();
        File outFile = fs.getLocalFile().getFile();
        String nodeId = fs.getNodeName();

        SinkWriterRuntimeFactory runtime = new SinkWriterRuntimeFactory(printColumns, printerFactories, outFile,
                getWriterFactory(), inputDesc);
        AlgebricksPartitionConstraint apc = new AlgebricksAbsolutePartitionConstraint(new String[] { nodeId });
        return new Pair<IPushRuntimeFactory, AlgebricksPartitionConstraint>(runtime, apc);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getResultHandleRuntime(IDataSink sink,
            int[] printColumns, IPrinterFactory[] printerFactories, RecordDescriptor inputDesc, boolean ordered,
            JobSpecification spec) throws AlgebricksException {
        ResultSetDataSink rsds = (ResultSetDataSink) sink;
        ResultSetSinkId rssId = (ResultSetSinkId) rsds.getId();
        ResultSetId rsId = rssId.getResultSetId();

        ResultWriterOperatorDescriptor resultWriter = null;
        try {
            IResultSerializerFactory resultSerializedAppenderFactory = resultSerializerFactoryProvider
                    .getAqlResultSerializerFactoryProvider(printColumns, printerFactories, getWriterFactory());
            resultWriter = new ResultWriterOperatorDescriptor(spec, rsId, ordered, getResultAsyncMode(),
                    resultSerializedAppenderFactory);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(resultWriter, null);
    }

    @Override
    public IDataSourceIndex<String, AqlSourceId> findDataSourceIndex(String indexId, AqlSourceId dataSourceId)
            throws AlgebricksException {
        AqlDataSource ads = findDataSource(dataSourceId);
        Dataset dataset = ((DatasetDataSource) ads).getDataset();

        try {
            String indexName = (String) indexId;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            if (secondaryIndex != null) {
                return new AqlIndex(secondaryIndex, dataset.getDataverseName(), dataset.getDatasetName(), this);
            } else {
                Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                        dataset.getDatasetName(), dataset.getDatasetName());
                if (primaryIndex.getIndexName().equals(indexId)) {
                    return new AqlIndex(primaryIndex, dataset.getDataverseName(), dataset.getDatasetName(), this);
                } else {
                    return null;
                }
            }
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public AqlDataSource lookupSourceInMetadata(AqlSourceId aqlId) throws AlgebricksException, MetadataException {
        Dataset dataset = findDataset(aqlId.getDataverseName(), aqlId.getDatasetName());
        if (dataset == null) {
            throw new AlgebricksException("Datasource with id " + aqlId + " was not found.");
        }
        String tName = dataset.getItemTypeName();
        IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, aqlId.getDataverseName(), tName).getDatatype();
        AqlDataSourceType datasourceType = dataset.getDatasetType().equals(DatasetType.EXTERNAL) ? AqlDataSourceType.EXTERNAL_DATASET
                : AqlDataSourceType.INTERNAL_DATASET;
        return new DatasetDataSource(aqlId, aqlId.getDataverseName(), aqlId.getDatasetName(), itemType, datasourceType);
    }

    @Override
    public boolean scannerOperatorIsLeaf(IDataSource<AqlSourceId> dataSource) {
        boolean result = false;
        switch (((AqlDataSource) dataSource).getDatasourceType()) {
            case INTERNAL_DATASET:
            case EXTERNAL_DATASET:
                result = ((DatasetDataSource) dataSource).getDataset().getDatasetType() == DatasetType.EXTERNAL;
                break;
            case FEED:
                result = true;
                break;
        }
        return result;
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getWriteResultRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, List<LogicalVariable> keys,
            LogicalVariable payload, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String dataverseName = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // move key fields to front
        int[] fieldPermutation = new int[numKeys + 1];
        int[] bloomFilterKeyFields = new int[numKeys];
        // System.arraycopy(keys, 0, fieldPermutation, 0, numKeys);
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }

        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            String itemTypeName = dataset.getItemTypeName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                    dataset.getDataverseName(), itemTypeName).getDatatype();
            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());

            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName);
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();

            long numElementsHint = getCardinalityPerPartitionHint(dataset);

            // TODO
            // figure out the right behavior of the bulkload and then give the
            // right callback
            // (ex. what's the expected behavior when there is an error during
            // bulkload?)
            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                    appContext.getStorageManagerInterface(), appContext.getIndexLifecycleManagerProvider(),
                    splitsAndConstraint.first, typeTraits, comparatorFactories, bloomFilterKeyFields, fieldPermutation,
                    GlobalConfig.DEFAULT_BTREE_FILL_FACTOR, false, numElementsHint, true,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                            compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), true),
                    NoOpOperationCallbackFactory.INSTANCE);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad,
                    splitsAndConstraint.second);
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertOrDeleteRuntime(IndexOperation indexOp,
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        String datasetName = dataSource.getId().getDatasetName();
        int numKeys = keys.size();
        // Move key fields to front.
        int[] fieldPermutation = new int[numKeys + 1];
        int[] bloomFilterKeyFields = new int[numKeys];
        int i = 0;
        for (LogicalVariable varKey : keys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        fieldPermutation[numKeys] = propagatedSchema.findVariable(payload);

        Dataset dataset = findDataset(dataSource.getId().getDataverseName(), datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse "
                    + dataSource.getId().getDataverseName());
        }
        try {
            Index primaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), dataset.getDatasetName());
            String indexName = primaryIndex.getIndexName();

            String itemTypeName = dataset.getItemTypeName();
            ARecordType itemType = (ARecordType) MetadataManager.INSTANCE.getDatatype(mdTxnCtx,
                    dataSource.getId().getDataverseName(), itemTypeName).getDatatype();

            ITypeTraits[] typeTraits = DatasetUtils.computeTupleTypeTraits(dataset, itemType);

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            IBinaryComparatorFactory[] comparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(dataset,
                    itemType, context.getBinaryComparatorFactoryProvider());
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForDataset(
                    dataSource.getId().getDataverseName(), datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            int[] primaryKeyFields = new int[numKeys];
            for (i = 0; i < numKeys; i++) {
                primaryKeyFields[i] = i;
            }
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            PrimaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new PrimaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, primaryKeyFields, txnSubsystemProvider, indexOp, ResourceType.LSM_BTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor insertDeleteOp = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, bloomFilterKeyFields, fieldPermutation, indexOp,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                            compactionInfo.first, compactionInfo.second, new PrimaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), true), null, modificationCallbackFactory, true,
                    indexName);

            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(insertDeleteOp,
                    splitsAndConstraint.second);

        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInsertRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.INSERT, dataSource, propagatedSchema, typeEnv, keys, payload,
                recordDesc, context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getDeleteRuntime(
            IDataSource<AqlSourceId> dataSource, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> keys, LogicalVariable payload, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec) throws AlgebricksException {
        return getInsertOrDeleteRuntime(IndexOperation.DELETE, dataSource, propagatedSchema, typeEnv, keys, payload,
                recordDesc, context, spec);
    }

    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertOrDeleteRuntime(
            IndexOperation indexOp, IDataSourceIndex<String, AqlSourceId> dataSourceIndex,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr,
            RecordDescriptor recordDesc, JobGenContext context, JobSpecification spec) throws AlgebricksException {
        String indexName = dataSourceIndex.getId();
        String dataverseName = dataSourceIndex.getDataSource().getId().getDataverseName();
        String datasetName = dataSourceIndex.getDataSource().getId().getDatasetName();

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        Index secondaryIndex;
        try {
            secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        AsterixTupleFilterFactory filterFactory = createTupleFilterFactory(inputSchemas, typeEnv, filterExpr, context);
        switch (secondaryIndex.getIndexType()) {
            case BTREE: {
                return getBTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp);
            }
            case RTREE: {
                return getRTreeDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp);
            }
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return getInvertedIndexDmlRuntime(dataverseName, datasetName, indexName, propagatedSchema, typeEnv,
                        primaryKeys, secondaryKeys, filterFactory, recordDesc, context, spec, indexOp,
                        secondaryIndex.getIndexType());
            }
            default: {
                throw new AlgebricksException("Insert and delete not implemented for index type: "
                        + secondaryIndex.getIndexType());
            }
        }
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexInsertRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.INSERT, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
    }

    @Override
    public Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getIndexDeleteRuntime(
            IDataSourceIndex<String, AqlSourceId> dataSourceIndex, IOperatorSchema propagatedSchema,
            IOperatorSchema[] inputSchemas, IVariableTypeEnvironment typeEnv, List<LogicalVariable> primaryKeys,
            List<LogicalVariable> secondaryKeys, ILogicalExpression filterExpr, RecordDescriptor recordDesc,
            JobGenContext context, JobSpecification spec) throws AlgebricksException {
        return getIndexInsertOrDeleteRuntime(IndexOperation.DELETE, dataSourceIndex, propagatedSchema, inputSchemas,
                typeEnv, primaryKeys, secondaryKeys, filterExpr, recordDesc, context, spec);
    }

    private AsterixTupleFilterFactory createTupleFilterFactory(IOperatorSchema[] inputSchemas,
            IVariableTypeEnvironment typeEnv, ILogicalExpression filterExpr, JobGenContext context)
            throws AlgebricksException {
        // No filtering condition.
        if (filterExpr == null) {
            return null;
        }
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        IScalarEvaluatorFactory filterEvalFactory = expressionRuntimeProvider.createEvaluatorFactory(filterExpr,
                typeEnv, inputSchemas, context);
        return new AsterixTupleFilterFactory(filterEvalFactory, context.getBinaryBooleanInspectorFactory());
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getBTreeDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp) throws AlgebricksException {

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        // generate field permutations
        int[] fieldPermutation = new int[numKeys];
        int[] bloomFilterKeyFields = new int[secondaryKeys.size()];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            bloomFilterKeyFields[i] = i;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numKeys];
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(i)
                        .toString(), recType);
                IAType keyType = keyPairType.first;
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        keyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp,
                    ResourceType.LSM_BTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor btreeBulkLoad = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, bloomFilterKeyFields, fieldPermutation, indexOp,
                    new LSMBTreeDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(datasetId),
                            compactionInfo.first, compactionInfo.second, new SecondaryIndexOperationTrackerProvider(
                                    dataset.getDatasetId()), AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMBTreeIOOperationCallbackFactory.INSTANCE, storageProperties
                                    .getBloomFilterFalsePositiveRate(), false), filterFactory,
                    modificationCallbackFactory, false, indexName);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(btreeBulkLoad,
                    splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getInvertedIndexDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp, IndexType indexType) throws AlgebricksException {

        // Sanity checks.
        if (primaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create inverted index on dataset with composite primary key.");
        }
        if (secondaryKeys.size() > 1) {
            throw new AlgebricksException("Cannot create composite inverted index on multiple fields.");
        }

        int numKeys = primaryKeys.size() + secondaryKeys.size();
        // generate field permutations
        int[] fieldPermutation = new int[numKeys];
        int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
        int i = 0;
        int j = 0;
        for (LogicalVariable varKey : secondaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            i++;
        }
        for (LogicalVariable varKey : primaryKeys) {
            int idx = propagatedSchema.findVariable(varKey);
            fieldPermutation[i] = idx;
            modificationCallbackPrimaryKeyFields[j] = i;
            i++;
            j++;
        }

        boolean isPartitioned;
        if (indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX) {
            isPartitioned = true;
        } else {
            isPartitioned = false;
        }

        Dataset dataset = findDataset(dataverseName, datasetName);
        if (dataset == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName + " in dataverse " + dataverseName);
        }
        String itemTypeName = dataset.getItemTypeName();
        IAType itemType;
        try {
            itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataset.getDataverseName(), itemTypeName)
                    .getDatatype();

            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }

            ARecordType recType = (ARecordType) itemType;

            // Index parameters.
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);

            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();

            int numTokenFields = (!isPartitioned) ? secondaryKeys.size() : secondaryKeys.size() + 1;
            ITypeTraits[] tokenTypeTraits = new ITypeTraits[numTokenFields];
            ITypeTraits[] invListsTypeTraits = new ITypeTraits[primaryKeys.size()];
            IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numTokenFields];
            IBinaryComparatorFactory[] invListComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, context.getBinaryComparatorFactoryProvider());

            IAType secondaryKeyType = null;
            for (i = 0; i < secondaryKeys.size(); ++i) {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(i)
                        .toString(), recType);
                secondaryKeyType = keyPairType.first;
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            i = 0;
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
                invListsTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            tokenComparatorFactories[0] = NonTaggedFormatUtil.getTokenBinaryComparatorFactory(secondaryKeyType);
            tokenTypeTraits[0] = NonTaggedFormatUtil.getTokenTypeTrait(secondaryKeyType);
            if (isPartitioned) {
                // The partitioning field is hardcoded to be a short *without*
                // an Asterix type tag.
                tokenComparatorFactories[1] = PointableBinaryComparatorFactory.of(ShortPointable.FACTORY);
                tokenTypeTraits[1] = ShortPointable.TYPE_TRAITS;
            }
            IBinaryTokenizerFactory tokenizerFactory = NonTaggedFormatUtil.getBinaryTokenizerFactory(
                    secondaryKeyType.getTypeTag(), indexType, secondaryIndex.getGramLength());

            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName);

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp,
                    ResourceType.LSM_INVERTED_INDEX);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            IIndexDataflowHelperFactory indexDataFlowFactory;
            if (!isPartitioned) {
                indexDataFlowFactory = new LSMInvertedIndexDataflowHelperFactory(new AsterixVirtualBufferCacheProvider(
                        datasetId), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate());
            } else {
                indexDataFlowFactory = new PartitionedLSMInvertedIndexDataflowHelperFactory(
                        new AsterixVirtualBufferCacheProvider(datasetId), compactionInfo.first, compactionInfo.second,
                        new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                        AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                        LSMInvertedIndexIOOperationCallbackFactory.INSTANCE,
                        storageProperties.getBloomFilterFalsePositiveRate());
            }
            AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor insertDeleteOp = new AsterixLSMInvertedIndexInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(), splitsAndConstraint.first,
                    appContext.getIndexLifecycleManagerProvider(), tokenTypeTraits, tokenComparatorFactories,
                    invListsTypeTraits, invListComparatorFactories, tokenizerFactory, fieldPermutation, indexOp,
                    indexDataFlowFactory, filterFactory, modificationCallbackFactory, indexName);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(insertDeleteOp,
                    splitsAndConstraint.second);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    private Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> getRTreeDmlRuntime(String dataverseName,
            String datasetName, String indexName, IOperatorSchema propagatedSchema, IVariableTypeEnvironment typeEnv,
            List<LogicalVariable> primaryKeys, List<LogicalVariable> secondaryKeys,
            AsterixTupleFilterFactory filterFactory, RecordDescriptor recordDesc, JobGenContext context,
            JobSpecification spec, IndexOperation indexOp) throws AlgebricksException {
        try {
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, itemTypeName).getDatatype();
            if (itemType.getTypeTag() != ATypeTag.RECORD) {
                throw new AlgebricksException("Only record types can be indexed.");
            }
            ARecordType recType = (ARecordType) itemType;
            Index secondaryIndex = MetadataManager.INSTANCE.getIndex(mdTxnCtx, dataset.getDataverseName(),
                    dataset.getDatasetName(), indexName);
            List<String> secondaryKeyExprs = secondaryIndex.getKeyFieldNames();
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(secondaryKeyExprs.get(0), recType);
            IAType spatialType = keyPairType.first;
            int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
            int numSecondaryKeys = dimension * 2;
            int numPrimaryKeys = primaryKeys.size();
            int numKeys = numSecondaryKeys + numPrimaryKeys;
            ITypeTraits[] typeTraits = new ITypeTraits[numKeys];
            IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];
            int[] fieldPermutation = new int[numKeys];
            int[] modificationCallbackPrimaryKeyFields = new int[primaryKeys.size()];
            int i = 0;
            int j = 0;

            for (LogicalVariable varKey : secondaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                i++;
            }
            for (LogicalVariable varKey : primaryKeys) {
                int idx = propagatedSchema.findVariable(varKey);
                fieldPermutation[i] = idx;
                modificationCallbackPrimaryKeyFields[j] = i;
                i++;
                j++;
            }
            IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
            IPrimitiveValueProviderFactory[] valueProviderFactories = new IPrimitiveValueProviderFactory[numSecondaryKeys];
            for (i = 0; i < numSecondaryKeys; i++) {
                comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                        nestedKeyType, true);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
                valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;
            }
            List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (String partitioningKey : partitioningKeys) {
                IAType keyType = recType.getFieldType(partitioningKey);
                typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
                ++i;
            }

            IBinaryComparatorFactory[] primaryComparatorFactories = DatasetUtils.computeKeysBinaryComparatorFactories(
                    dataset, recType, context.getBinaryComparatorFactoryProvider());
            IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitsAndConstraint = splitProviderAndPartitionConstraintsForDataset(
                    dataverseName, datasetName, indexName);
            int[] btreeFields = new int[primaryComparatorFactories.length];
            for (int k = 0; k < btreeFields.length; k++) {
                btreeFields[k] = k + numSecondaryKeys;
            }

            // prepare callback
            JobId jobId = ((JobEventListenerFactory) spec.getJobletEventListenerFactory()).getJobId();
            int datasetId = dataset.getDatasetId();
            TransactionSubsystemProvider txnSubsystemProvider = new TransactionSubsystemProvider();
            SecondaryIndexModificationOperationCallbackFactory modificationCallbackFactory = new SecondaryIndexModificationOperationCallbackFactory(
                    jobId, datasetId, modificationCallbackPrimaryKeyFields, txnSubsystemProvider, indexOp,
                    ResourceType.LSM_RTREE);

            Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo = DatasetUtils.getMergePolicyFactory(
                    dataset, mdTxnCtx);
            AsterixLSMTreeInsertDeleteOperatorDescriptor rtreeUpdate = new AsterixLSMTreeInsertDeleteOperatorDescriptor(
                    spec, recordDesc, appContext.getStorageManagerInterface(),
                    appContext.getIndexLifecycleManagerProvider(), splitsAndConstraint.first, typeTraits,
                    comparatorFactories, null, fieldPermutation, indexOp, new LSMRTreeDataflowHelperFactory(
                            valueProviderFactories, RTreePolicyType.RTREE, primaryComparatorFactories,
                            new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), compactionInfo.first,
                            compactionInfo.second, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            LSMRTreeIOOperationCallbackFactory.INSTANCE, proposeLinearizer(nestedKeyType.getTypeTag(),
                                    comparatorFactories.length), storageProperties.getBloomFilterFalsePositiveRate(),
                            btreeFields), filterFactory, modificationCallbackFactory, false, indexName);
            return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(rtreeUpdate, splitsAndConstraint.second);
        } catch (MetadataException | IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public JobId getJobId() {
        return jobId;
    }

    public static ITreeIndexFrameFactory createBTreeNSMInteriorFrameFactory(ITypeTraits[] typeTraits) {
        return new BTreeNSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    public static ILinearizeComparatorFactory proposeLinearizer(ATypeTag keyType, int numKeyFields)
            throws AlgebricksException {
        if (numKeyFields / 2 == 2 && (keyType == ATypeTag.DOUBLE)) {
            return new HilbertDoubleComparatorFactory(2);
        } else if (keyType == ATypeTag.DOUBLE) {
            return new ZCurveDoubleComparatorFactory(numKeyFields / 2);
        } else if (keyType == ATypeTag.INT8 || keyType == ATypeTag.INT16 || keyType == ATypeTag.INT32
                || keyType == ATypeTag.INT64) {
            return new ZCurveIntComparatorFactory(numKeyFields / 2);
        } else {
            throw new AlgebricksException("Cannot propose linearizer for key with type " + keyType + ".");
        }
    }

    /**
     * Calculate an estimate size of the bloom filter. Note that this is an
     * estimation which assumes that the data is going to be uniformly
     * distributed across all partitions.
     * 
     * @param dataset
     * @return Number of elements that will be used to create a bloom filter per
     *         dataset per partition
     * @throws MetadataException
     * @throws AlgebricksException
     */
    public long getCardinalityPerPartitionHint(Dataset dataset) throws MetadataException, AlgebricksException {
        String numElementsHintString = dataset.getHints().get(DatasetCardinalityHint.NAME);
        long numElementsHint;
        if (numElementsHintString == null) {
            numElementsHint = DatasetCardinalityHint.DEFAULT;
        } else {
            numElementsHint = Long.parseLong(numElementsHintString);
        }
        int numPartitions = 0;
        List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx,
                dataset.getDatasetDetails().getNodeGroupName()).getNodeNames();
        for (String nd : nodeGroup) {
            numPartitions += AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nd);
        }
        return numElementsHint /= numPartitions;
    }

    @Override
    public IFunctionInfo lookupFunction(FunctionIdentifier fid) {
        return AsterixBuiltinFunctions.lookupFunction(fid);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataset(
            String dataverseName, String datasetName, String targetIdxName) throws AlgebricksException {
        FileSplit[] splits = splitsForDataset(mdTxnCtx, dataverseName, datasetName, targetIdxName);
        return splitProviderAndPartitionConstraints(splits);
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForDataverse(
            String dataverse) {
        FileSplit[] splits = splitsForDataverse(mdTxnCtx, dataverse);
        return splitProviderAndPartitionConstraints(splits);
    }

    private Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraints(
            FileSplit[] splits) {
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        String[] loc = new String[splits.length];
        for (int p = 0; p < splits.length; p++) {
            loc[p] = splits[p].getNodeName();
        }
        AlgebricksPartitionConstraint pc = new AlgebricksAbsolutePartitionConstraint(loc);
        return new Pair<IFileSplitProvider, AlgebricksPartitionConstraint>(splitProvider, pc);
    }

    private FileSplit[] splitsForDataverse(MetadataTransactionContext mdTxnCtx, String dataverseName) {
        File relPathFile = new File(dataverseName);
        List<FileSplit> splits = new ArrayList<FileSplit>();
        for (Map.Entry<String, String[]> entry : stores.entrySet()) {
            String node = entry.getKey();
            String[] nodeStores = entry.getValue();
            if (nodeStores == null) {
                continue;
            }
            for (int i = 0; i < nodeStores.length; i++) {
                int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(node);
                String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(node);
                for (int j = 0; j < nodeStores.length; j++) {
                    for (int k = 0; k < numIODevices; k++) {
                        File f = new File(ioDevices[k] + File.separator + nodeStores[j] + File.separator + relPathFile);
                        splits.add(new FileSplit(node, new FileReference(f), k));
                    }
                }
            }
        }
        return splits.toArray(new FileSplit[] {});
    }

    private FileSplit[] splitsForDataset(MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName,
            String targetIdxName) throws AlgebricksException {
        try {
            File relPathFile = new File(getRelativePath(dataverseName, datasetName + "_idx_" + targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            IDatasetDetails datasetDetails = dataset.getDatasetDetails();
            List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, datasetDetails.getNodeGroupName())
                    .getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + datasetDetails.getNodeGroupName());
            }

            List<FileSplit> splitArray = new ArrayList<FileSplit>();
            for (String nd : nodeGroup) {
                String[] nodeStores = stores.get(nd);
                if (nodeStores == null) {
                    LOGGER.warning("Node " + nd + " has no stores.");
                    throw new AlgebricksException("Node " + nd + " has no stores.");
                } else {
                    int numIODevices;
                    if (datasetDetails.getNodeGroupName().compareTo(MetadataConstants.METADATA_NODEGROUP_NAME) == 0) {
                        numIODevices = 1;
                    } else {
                        numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nd);
                    }
                    String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(nd);
                    for (int j = 0; j < nodeStores.length; j++) {
                        for (int k = 0; k < numIODevices; k++) {
                            File f = new File(ioDevices[k] + File.separator + nodeStores[j] + File.separator
                                    + relPathFile);
                            splitArray.add(new FileSplit(nd, new FileReference(f), k));
                        }
                    }
                }
            }
            FileSplit[] splits = new FileSplit[splitArray.size()];
            int i = 0;
            for (FileSplit fs : splitArray) {
                splits[i++] = fs;
            }
            return splits;
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    private static Map<String, String> initializeAdapterFactoryMapping() {
        Map<String, String> adapterFactoryMapping = new HashMap<String, String>();
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.NCFileSystemAdapter",
                "edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter",
                "edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.RSSFeedAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter..RSSFeedAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapter",
                "edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapterFactory");
        adapterFactoryMapping.put("edu.uci.ics.asterix.tools.external.data.RateControlledFileSystemBasedAdapter",
                "edu.uci.ics.asterix.tools.external.data.RateControlledFileSystemBasedAdapterFactory");

        return adapterFactoryMapping;
    }

    public DatasourceAdapter getAdapter(MetadataTransactionContext mdTxnCtx, String dataverseName, String adapterName)
            throws MetadataException {
        DatasourceAdapter adapter = null;
        // search in default namespace (built-in adapter)
        adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);

        // search in dataverse (user-defined adapter)
        if (adapter == null) {
            adapter = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, dataverseName, adapterName);
        }
        return adapter;
    }

    private static String getRelativePath(String dataverseName, String fileName) {
        return dataverseName + File.separator + fileName;
    }

    public Dataset findDataset(String dataverse, String dataset) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverse, dataset);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public IAType findType(String dataverse, String typeName) {
        Datatype type;
        try {
            type = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverse, typeName);
        } catch (Exception e) {
            throw new IllegalStateException();
        }
        if (type == null) {
            throw new IllegalStateException();
        }
        return type.getDatatype();
    }

    public Feed findFeed(String dataverse, String feedName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getFeed(mdTxnCtx, dataverse, feedName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public FeedPolicy findFeedPolicy(String dataverse, String policyName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getFeedPolicy(mdTxnCtx, dataverse, policyName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public List<Index> getDatasetIndexes(String dataverseName, String datasetName) throws AlgebricksException {
        try {
            return MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName, datasetName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        ArrayList<String> locs = new ArrayList<String>();
        for (String i : stores.keySet()) {
            String[] nodeStores = stores.get(i);
            int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(i);
            for (int j = 0; j < nodeStores.length; j++) {
                for (int k = 0; k < numIODevices; k++) {
                    locs.add(i);
                }
            }
        }
        String[] cluster = new String[locs.size()];
        cluster = locs.toArray(cluster);
        return new AlgebricksAbsolutePartitionConstraint(cluster);
    }

    public IDataFormat getFormat() {
        return FormatUtils.getDefaultFormat();
    }

    /**
     * Add HDFS scheduler and the cluster location constraint into the scheduler
     * 
     * @param properties
     *            the original dataset properties
     * @return a new map containing the original dataset properties and the
     *         scheduler/locations
     */
    private Map<String, Object> wrapProperties(Map<String, String> properties) {
        Map<String, Object> wrappedProperties = new HashMap<String, Object>();
        wrappedProperties.putAll(properties);
        // wrappedProperties.put(SCHEDULER, hdfsScheduler);
        // wrappedProperties.put(CLUSTER_LOCATIONS, getClusterLocations());
        return wrappedProperties;
    }

    /**
     * Adapt the original properties to a string-object map
     * 
     * @param properties
     *            the original properties
     * @return the new stirng-object map
     */
    private Map<String, Object> wrapPropertiesEmpty(Map<String, String> properties) {
        Map<String, Object> wrappedProperties = new HashMap<String, Object>();
        wrappedProperties.putAll(properties);
        return wrappedProperties;
    }

    public Pair<IFileSplitProvider, AlgebricksPartitionConstraint> splitProviderAndPartitionConstraintsForFilesIndex(
            String dataverseName, String datasetName, String targetIdxName, boolean create) throws AlgebricksException {
        FileSplit[] splits = splitsForFilesIndex(mdTxnCtx, dataverseName, datasetName, targetIdxName, create);
        return splitProviderAndPartitionConstraints(splits);
    }

    private FileSplit[] splitsForFilesIndex(MetadataTransactionContext mdTxnCtx, String dataverseName,
            String datasetName, String targetIdxName, boolean create) throws AlgebricksException {

        try {
            File relPathFile = new File(getRelativePath(dataverseName, datasetName + "_idx_" + targetIdxName));
            Dataset dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx, dataverseName, datasetName);
            ExternalDatasetDetails datasetDetails = (ExternalDatasetDetails) dataset.getDatasetDetails();
            List<String> nodeGroup = MetadataManager.INSTANCE.getNodegroup(mdTxnCtx, datasetDetails.getNodeGroupName())
                    .getNodeNames();
            if (nodeGroup == null) {
                throw new AlgebricksException("Couldn't find node group " + datasetDetails.getNodeGroupName());
            }

            List<FileSplit> splitArray = new ArrayList<FileSplit>();
            for (String nd : nodeGroup) {
                String[] nodeStores = stores.get(nd);
                if (nodeStores == null) {
                    LOGGER.warning("Node " + nd + " has no stores.");
                    throw new AlgebricksException("Node " + nd + " has no stores.");
                } else {
                    // Only the first partition when create
                    String[] ioDevices = AsterixClusterProperties.INSTANCE.getIODevices(nd);
                    if (create) {
                        for (int j = 0; j < nodeStores.length; j++) {
                            File f = new File(ioDevices[0] + File.separator + nodeStores[j] + File.separator
                                    + relPathFile);
                            splitArray.add(new FileSplit(nd, new FileReference(f), 0));
                        }
                    } else {
                        int numIODevices = AsterixClusterProperties.INSTANCE.getNumberOfIODevices(nd);
                        for (int j = 0; j < nodeStores.length; j++) {
                            for (int k = 0; k < numIODevices; k++) {
                                File f = new File(ioDevices[0] + File.separator + nodeStores[j] + File.separator
                                        + relPathFile);
                                splitArray.add(new FileSplit(nd, new FileReference(f), 0));
                            }
                        }
                    }
                }
            }
            FileSplit[] splits = new FileSplit[splitArray.size()];
            int i = 0;
            for (FileSplit fs : splitArray) {
                splits[i++] = fs;
            }
            return splits;
        } catch (MetadataException me) {
            throw new AlgebricksException(me);
        }
    }

    public AsterixStorageProperties getStorageProperties() {
        return storageProperties;
    }

    public Map<String, Integer> getLocks() {
        return locks;
    }

    public void setLocks(Map<String, Integer> locks) {
        this.locks = locks;
    }

}