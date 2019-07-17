/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser.Column_nameContext;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

public class CreateTableParserListener extends BaseParserListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableParserListener.class);

    private final List<ParseTreeListener> listeners;
    private TableEditor tableEditor;
    private String catalogName;
    private String schemaName;
    private OracleDdlParser parser;
    private ColumnDefinitionParserListener columnDefinitionParserListener;
    private List<String> uniqueColumns;

    CreateTableParserListener(final String catalogName, final String schemaName, final OracleDdlParser parser,
                                     final List<ParseTreeListener> listeners) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterCreate_table(PlSqlParser.Create_tableContext ctx) {
        if (ctx.relational_table() == null) {
            throw new IllegalArgumentException("Only relational tables are supported");
        }
        TableId tableId = new TableId(catalogName, schemaName, getTableName(ctx.tableview_name()));
        tableEditor = parser.databaseTables().editOrCreateTable(tableId);
        uniqueColumns = new ArrayList<>();
        super.enterCreate_table(ctx);
    }

    @Override
    public void exitCreate_table(PlSqlParser.Create_tableContext ctx) {
        if (!tableEditor.hasPrimaryKey() && !uniqueColumns.isEmpty()) {
            LOGGER.info("Table '{}' don't have primary key but has unique key '{}', using it as primary key", tableEditor.tableId(), uniqueColumns);
            tableEditor.setPrimaryKeyNames(uniqueColumns);
        }
        Table table = getTable();
        assert table != null;
        parser.runIfNotNull(() -> {
            listeners.remove(columnDefinitionParserListener);
            columnDefinitionParserListener = null;
            parser.databaseTables().overwriteTable(table);
            //parser.signalCreateTable(tableEditor.tableId(), ctx); todo ?
        }, tableEditor, table);
        super.exitCreate_table(ctx);
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> {
            String columnName = getColumnName(ctx.column_name());
            ColumnEditor columnEditor = Column.editor().name(columnName);
            if (columnDefinitionParserListener == null) {
                columnDefinitionParserListener = new ColumnDefinitionParserListener(tableEditor, columnEditor, parser.dataTypeResolver());
                // todo: this explicit call is for the first column, should it be fixed?
                columnDefinitionParserListener.enterColumn_definition(ctx);
                listeners.add(columnDefinitionParserListener);
            } else {
                columnDefinitionParserListener.setColumnEditor(columnEditor);
            }
        }, tableEditor);
        super.enterColumn_definition(ctx);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(() -> tableEditor.addColumn(columnDefinitionParserListener.getColumn()),
                tableEditor, columnDefinitionParserListener);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void exitOut_of_line_constraint(PlSqlParser.Out_of_line_constraintContext ctx) {
        if (ctx.PRIMARY() != null) {
            final List<String> pkColumnNames = extractColumnNames(ctx.column_name());

            tableEditor.setPrimaryKeyNames(pkColumnNames);
        }
        else if (ctx.UNIQUE() != null && uniqueColumns.isEmpty()) {
            uniqueColumns.addAll(extractColumnNames(ctx.column_name()));
        }
        super.exitOut_of_line_constraint(ctx);
    }

    private List<String> extractColumnNames(List<Column_nameContext> ctx) {
        return ctx.stream()
                .map(this::getColumnName)
                .collect(Collectors.toList());
    }

    private Table getTable() {
        return tableEditor != null ? tableEditor.create() : null;
    }
}
