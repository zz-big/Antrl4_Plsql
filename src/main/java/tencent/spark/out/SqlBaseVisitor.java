// Generated from D:/Development/AntlrTest/src\SqlBase.g4 by ANTLR 4.5.3
package tencent.spark.out;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SqlBaseVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleDataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUse(SqlBaseParser.UseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createDatabase}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateDatabase(SqlBaseParser.CreateDatabaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setDatabaseProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetDatabaseProperties(SqlBaseParser.SetDatabasePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropDatabase}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropDatabase(SqlBaseParser.DropDatabaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTableUsing}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableUsing(SqlBaseParser.CreateTableUsingContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTable(SqlBaseParser.CreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnalyze(SqlBaseParser.AnalyzeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameTable(SqlBaseParser.RenameTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableLocation(SqlBaseParser.SetTableLocationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTable(SqlBaseParser.DropTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateView(SqlBaseParser.CreateViewContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropFunction(SqlBaseParser.DropFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showDatabases}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowDatabases(SqlBaseParser.ShowDatabasesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowPartitions(SqlBaseParser.ShowPartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeDatabase}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeDatabase(SqlBaseParser.DescribeDatabaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeTable(SqlBaseParser.DescribeTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefreshTable(SqlBaseParser.RefreshTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefreshResource(SqlBaseParser.RefreshResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCacheTable(SqlBaseParser.CacheTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUncacheTable(SqlBaseParser.UncacheTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitClearCache(SqlBaseParser.ClearCacheContext ctx);
	/**
	 * Visit a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoadData(SqlBaseParser.LoadDataContext ctx);
	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTruncateTable(SqlBaseParser.TruncateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepairTable(SqlBaseParser.RepairTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitManageResource(SqlBaseParser.ManageResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetConfiguration(SqlBaseParser.SetConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createTableHeader}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#bucketSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBucketSpec(SqlBaseParser.BucketSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#skewSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSkewSpec(SqlBaseParser.SkewSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#locationSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLocationSpec(SqlBaseParser.LocationSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertInto(SqlBaseParser.InsertIntoContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionSpec(SqlBaseParser.PartitionSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionVal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionVal(SqlBaseParser.PartitionValContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#describeFuncName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#describeColName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeColName(SqlBaseParser.DescribeColNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#ctes}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCtes(SqlBaseParser.CtesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedQuery(SqlBaseParser.NamedQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProvider}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProvider(SqlBaseParser.TableProviderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tablePropertyList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTablePropertyList(SqlBaseParser.TablePropertyListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProperty}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProperty(SqlBaseParser.TablePropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tablePropertyKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTablePropertyKey(SqlBaseParser.TablePropertyKeyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tablePropertyValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTablePropertyValue(SqlBaseParser.TablePropertyValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#constantList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantList(SqlBaseParser.ConstantListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nestedConstantList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedConstantList(SqlBaseParser.NestedConstantListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createFileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableFileFormat(SqlBaseParser.TableFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#storageHandler}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStorageHandler(SqlBaseParser.StorageHandlerContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#resource}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResource(SqlBaseParser.ResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#queryNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#queryOrganization}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable(SqlBaseParser.TableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(SqlBaseParser.FromClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#aggregation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregation(SqlBaseParser.AggregationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#lateralView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLateralView(SqlBaseParser.LateralViewContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelation(SqlBaseParser.RelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinType(SqlBaseParser.JoinTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinCriteria}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sample}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSample(SqlBaseParser.SampleContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierList(SqlBaseParser.IdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierCommentList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierComment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx);
	/**
	 * Visit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#inlineTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTable(SqlBaseParser.InlineTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanDefault}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanDefault(SqlBaseParser.BooleanDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicated}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicated(SqlBaseParser.PredicatedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(SqlBaseParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDereference(SqlBaseParser.DereferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowConstructor(SqlBaseParser.RowConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStar(SqlBaseParser.StarContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timeFunctionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimeFunctionCall(SqlBaseParser.TimeFunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicateOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#intervalField}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalField(SqlBaseParser.IntervalFieldContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#intervalValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalValue(SqlBaseParser.IntervalValueContext ctx);
	/**
	 * Visit a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColTypeList(SqlBaseParser.ColTypeListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColType(SqlBaseParser.ColTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#complexColTypeList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#complexColType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexColType(SqlBaseParser.ComplexColTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windows}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindows(SqlBaseParser.WindowsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedWindow(SqlBaseParser.NamedWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowRef(SqlBaseParser.WindowRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowDef(SqlBaseParser.WindowDefContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(SqlBaseParser.NonReservedContext ctx);
}