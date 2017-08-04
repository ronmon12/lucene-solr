package org.apache.lucene.codecs.embeddeddb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Created by rlmathes on 7/15/17.
 *
 * Singleton class responsible for granting access to an embedded key/value database
 *
 */
public enum BerkeleyDBStore implements EmbeddedDBStore{

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private StoredClassCatalog storedClassCatalog;
    private final Properties properties = new Properties();
    private final String PATH_EMBEDDEDDB_STORE = "tmp_lucene_embedded_store_directory";

    private Database documentStoreDatabase;
    private EntryBinding documentKeyBinding;
    private EntryBinding documentDataBinding;
    private final String DBNAME_DOCUMENT_STORE = "document_store";

    BerkeleyDBStore() {
        reinitialize();
    }

    public void reinitialize() {
        initializeEnvironment();
        initializeDatabases();
    }

    private void initializeEnvironment() {

        /*
         *  Currently Berkeley will always start in in-memory mode. Need to either figure out how to introduce
         *  VM arguments to the lucene test framework, or provide a default configuration file that can be
         *  overriden to specify disk persistence
         *
         */
        properties.put(BerkeleyDBCoreConstants.LOG_MEM_ONLY, "true");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_CHECKPOINTER, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_CLEANER, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_EVICTOR, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_IN_COMPRESSOR, "false");
        Logger.LOG(LogLevel.INFO, "Starting Lucene embedded database in testing mode. " +
                    "Background threads and disk persistence disabled.");

        environmentConfig = new EnvironmentConfig(properties);
        environmentConfig.setAllowCreate(true);

        String luceneEmbeddedDBStoreDirectory = System.getProperty("luceneEmbeddedDBStoreDirectory");
        if(null == luceneEmbeddedDBStoreDirectory) {
            luceneEmbeddedDBStoreDirectory = PATH_EMBEDDEDDB_STORE;
        }
        final File storeFile = new File(luceneEmbeddedDBStoreDirectory);
        try {
            storeFile.mkdir();
            Logger.LOG(LogLevel.INFO, "Directory created for Lucene embedded database.");
        }
        catch(SecurityException e) {
            Logger.LOG(LogLevel.ERROR, "Security violation occurred while trying to create the embedded database directory.");
        }

        try {
            environment = new Environment(storeFile, environmentConfig);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Error occurred while trying to create the embedded database environment.");
        }
    }

    private void initializeDatabases() {
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);

        try {
            documentStoreDatabase = environment.openDatabase(null, DBNAME_DOCUMENT_STORE, databaseConfig);
            storedClassCatalog = new StoredClassCatalog(documentStoreDatabase);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to access the requested database from the environment.");
        }

        documentKeyBinding = new SerialBinding(storedClassCatalog, String.class);
        documentDataBinding = new SerialBinding(storedClassCatalog, EDBDocument.class);
    }

    public void put(final String documentKey, final EDBDocument document) {

        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStoreDatabase.put(null, entryKey, entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to insert entry into the document store.");
        }
    }

    public EDBDocument get(final String documentKey) {

        EDBDocument document = new EDBDocument();
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            document = (EDBDocument) documentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to retrieve requested document from document store.");
        }
        return document;
    }

    public void close() {

        try {
            documentStoreDatabase.close();
            environment.close();
            Logger.LOG(LogLevel.INFO, "Releasing resources for embedded database environment.");
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to release resources for embedded database environment.");
        }
    }

    public void purge() {
        try {
            documentStoreDatabase.close();
            environment.truncateDatabase(null, DBNAME_DOCUMENT_STORE, true);
            initializeDatabases();
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to truncate the document store");
        }
    }


    Database getStore() {
        return documentStoreDatabase;
    }

}
