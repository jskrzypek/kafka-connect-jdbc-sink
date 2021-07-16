/*
 *
 * Copyright 2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.eventstreams.connect.jdbcsink.database.writer;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class JDBCWriter implements IDatabaseWriter{

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);
    
    private static final String jsonColumnName = "json_object";

    private final IDataSource dataSource;

    public JDBCWriter(final IDataSource dataSource) {
        this.dataSource = dataSource;
    }

    private boolean doesTableExist(Connection connection, String tableName) throws SQLException {
        String[] tableParts = tableName.split("\\.");
        DatabaseMetaData dbm = connection.getMetaData();
        ResultSet table = dbm.getTables(null, tableParts[0], tableParts[1], null);
        return table.next();
    }

    // TODO: verify will work for all database flavors
    private String getDatabaseFieldType(Schema.Type type) {
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
            case FLOAT64:
                return "FLOAT";
            case BOOLEAN:
                return "BOOLEAN";
            default:
                return "VARCHAR(255)";
        }
    }

    public void oldcreateTable(Connection connection, String tableName, Schema schema) throws SQLException {

        // TODO: verify will work for all database flavors
        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        List<String> fieldDatabaseDefinitions = new ArrayList<>();
        // TODO: verify will work for all database flavors
        fieldDatabaseDefinitions.add("id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY");
        // Error from next line "Error: Cannot list fields on non-struct type"
        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            // TODO: verify will work for all database flavors
            String nullable = field.schema().isOptional() ? "" : " NOT NULL DEFAULT";
            fieldDatabaseDefinitions.add(String.format("%s %s%s", fieldName, getDatabaseFieldType(fieldType), nullable));
        }

        final String createQuery = String.format(CREATE_STATEMENT, tableName, String.join(", ", fieldDatabaseDefinitions));
        logger.info("CREATEQuery: " + createQuery);

        Statement statement = connection.createStatement();
        statement.execute(createQuery);
        logger.info("TABLE " + tableName + " has been created");
        statement.close();
    }
    
    // new version for JSON only, no schema
    public void createTable(Connection connection, String tableName) throws SQLException {

        // TODO: verify will work for all database flavors
        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        List<String> fieldDatabaseDefinitions = new ArrayList<>();
        // TODO: verify will work for all database flavors
        // TODO Change this to serial auto id thingy in Postgres?
        fieldDatabaseDefinitions.add("id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY");
        // Error from next line "Error: Cannot list fields on non-struct type"
        
        // TODO Hack - just add one column for json value as jsonb type
       
        // fieldDatabaseDefinitions.add(String.format("%s %s%s", "json_object", getDatabaseFieldType(fieldType), nullable));
        fieldDatabaseDefinitions.add(jsonColumnName + " jsonb NOT NULL");

        final String createQuery = String.format(CREATE_STATEMENT, tableName, String.join(", ", fieldDatabaseDefinitions));
        logger.info("CREATEQuery: " + createQuery);

        Statement statement = connection.createStatement();
        statement.execute(createQuery);
        logger.info("TABLE " + tableName + " has been created");
        
        // create index
        // CREATE INDEX gin_index ON tablename USING gin (columnName);
        
        // BUG index name must be unique! concat with table name

        final String INDEX_STATEMENT = "CREATE INDEX %s ON %s USING gin (%s)";
        // Posgres complains about dots in index name, replace with _
        String tableNameNoDots = tableName.replace('.', '_');
        String indexQuery = String.format(INDEX_STATEMENT, tableNameNoDots + "_gin_index", tableName, jsonColumnName); 
        logger.info("INDEXQuery: " + indexQuery);
        statement.execute(indexQuery);
        logger.info("INDEX gin_index has been created");
        
        statement.close();
    }


    // TODO: encode maps and lists as strings
    private String encodeValueForQuery(Object value) {
        return value instanceof String ? String.format("'%s'", value.toString()) : value.toString();
    }

    // @Override
    public void oldinsert(String tableName, Collection<SinkRecord> records) throws SQLException {
        Connection connection = null;
        // TODO: need an SQL statement builder with potential variations depending on the platform
        final String INSERT_STATEMENT = "INSERT INTO %s(%s) VALUES (%s)";
        try {
            connection = this.dataSource.getConnection();
            Statement statement = connection.createStatement();
            
            // fake schema
            Schema fake = SchemaBuilder.struct()
                    .name("com.instaclustr.paul").version(1).doc("Fake schema for PG test")
                    .field("name", Schema.STRING_SCHEMA)
                    .field("company", Schema.STRING_SCHEMA)
                    .build();

            if (!doesTableExist(connection, tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                logger.info("PAUL DEBUG: value = {} valueSchema = {}", records.iterator().next().value().toString(), records.iterator().next().valueSchema().toString());
                logger.info("PAUL DEBUG: fakeSchema = {}", fake.toString());

                // hack to provide schema here - better to get it from config file, this just for debugging
                
               
               //  createTable(connection, tableName, records.iterator().next().valueSchema());
                oldcreateTable(connection, tableName, fake);
            }

          //   List<String> fieldNames = records.iterator().next().valueSchema().fields().stream().map(Field::name).collect(Collectors.toList());

            List<String> fieldNames = fake.fields().stream().map(Field::name).collect(Collectors.toList());

            for (SinkRecord record: records) {
            	// blew up here now!
            	/* from docs:
            	 * Schema schema = SchemaBuilder.struct().name("com.example.Person")
             .field("name", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA).build()
         Struct struct = new Struct(schema).put("name", "Bobby McGee").put("age", 21)
            	 */
                Struct recordValue = (Struct) record.value();

                logger.debug(" --- Record Schema --- ");
                logger.debug(record.valueSchema().toString());
                logger.debug(" --- Record Value --- ");
                logger.debug(record.value().toString());
                logger.debug(" --- Record Headers --- ");
                logger.debug(record.headers().toString());

                List<String> fieldValues = fieldNames.stream().map(fieldName -> encodeValueForQuery(recordValue.get(fieldName))).collect(Collectors.toList());

                logger.debug("TableFields: "+ fieldNames.size());
                logger.debug("TableFields value: "+ fieldNames.toString());
                logger.debug("DataFields: "+ fieldValues.size());
                logger.debug("DataFields value: "+ fieldValues.toString());

                String listTableFields = String.join(", ", fieldNames);
                String listDataFields = String.join(", ", fieldValues);

                final String finalQuery = String.format(INSERT_STATEMENT, tableName, listTableFields, listDataFields);
                statement.addBatch(finalQuery);
                logger.debug("Final prepared statement: '{}' //", finalQuery);
            }

            statement.executeBatch();
            statement.close();

        } catch (BatchUpdateException batchUpdateException) {
            // TODO: write failed records from batch to kafka topic?
            logger.error("SOME OPERATIONS IN BATCH FAILED");
            logger.error(batchUpdateException.toString());
        } catch (SQLException sQLException){
            logger.error(sQLException.toString());
            throw sQLException;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    // New insert for JSON records
    // Steps: 
    // 1 Create table with id, timestamp, value (type jsonb) columns
    // 2 Check record.value() not empty and valid JSON
    // 3 Ideally check record.value() conforms to JSON scheman (from config file perhaps)
    // TODO What exceptions should be thrown if empty? not valid JSON? not JSON? 
    @Override
    public void insert(String tableName, Collection<SinkRecord> records) throws SQLException {
        Connection connection = null;
        // TODO: need an SQL statement builder with potential variations depending on the platform
        final String INSERT_STATEMENT = "INSERT INTO %s(%s) VALUES ('%s')";
        try {
            connection = this.dataSource.getConnection();
            Statement statement = connection.createStatement();
            
            // fake schema
            Schema fake = SchemaBuilder.struct()
                    .name("com.instaclustr.paul").version(1).doc("Fake schema for PG test")
                    .field("name", Schema.STRING_SCHEMA)
                    .field("company", Schema.STRING_SCHEMA)
                    .build();

            if (!doesTableExist(connection, tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                logger.info("PAUL DEBUG: value = {}", records.iterator().next().value().toString());
                                
               
               //  createTable(connection, tableName, records.iterator().next().valueSchema());
                // no schema in this json only version (yet)
                createTable(connection, tableName);
            }

          //   List<String> fieldNames = records.iterator().next().valueSchema().fields().stream().map(Field::name).collect(Collectors.toList());

            // List<String> fieldNames = fake.fields().stream().map(Field::name).collect(Collectors.toList());

            for (SinkRecord record: records) {
            	// blew up here now!
            	/* from docs:
            	 * Schema schema = SchemaBuilder.struct().name("com.example.Person")
             .field("name", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA).build()
         Struct struct = new Struct(schema).put("name", "Bobby McGee").put("age", 21)
            	 */
                // Struct recordValue = (Struct) record.value();

                
                logger.info(" --- Record Value --- ");
                logger.info(record.value().toString());
                logger.info(" --- Record Headers --- ");
                logger.info(record.headers().toString());

              //  List<String> fieldValues = fieldNames.stream().map(fieldName -> encodeValueForQuery(recordValue.get(fieldName))).collect(Collectors.toList());

                //logger.debug("TableFields: "+ fieldNames.size());
                //logger.debug("TableFields value: "+ fieldNames.toString());
                //logger.debug("DataFields: "+ fieldValues.size());
                //logger.debug("DataFields value: "+ fieldValues.toString());

                //String listTableFields = String.join(", ", fieldNames);
                //String listDataFields = String.join(", ", fieldValues);

                // Note that json values must be enclosed in ' ... ', done by INSERT_STATEMENT
                final String finalQuery = String.format(INSERT_STATEMENT, tableName, jsonColumnName, record.value().toString());
                statement.addBatch(finalQuery);
                logger.info("Final prepared statement: '{}' //", finalQuery);
            }

            statement.executeBatch();
            statement.close();

        } catch (BatchUpdateException batchUpdateException) {
            // TODO: write failed records from batch to kafka topic?
            logger.error("SOME OPERATIONS IN BATCH FAILED");
            logger.error(batchUpdateException.toString());
        } catch (SQLException sQLException){
            logger.error(sQLException.toString());
            throw sQLException;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
}
