package org.apache.lucene.codecs.embeddeddb;

import java.io.File;
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
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.StatsConfig;

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
 * Implementation of EmbeddedDBStore that utilizes BerkeleyDB for the storage of Lucene documents
 *
 */
public enum BerkeleyDBStore implements EmbeddedDBStore{

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private StoredClassCatalog storedClassCatalog;
    private final Properties properties = new Properties();
    private final String PATH_BERKELEYDB_DIRECTORY = "tmp_lucene_embedded_store_directory";

    private Database catalogDatabase;
    private DatabaseConfig catalogConfig;
    private final String DBNAME_CATALOG = "catalog";
    private EntryBinding documentKeyBinding;
    private EntryBinding documentDataBinding;
    private EntryBinding handleIndexKeyBinding;

    private Database documentStore;
    private DatabaseConfig databaseConfig;
    private final String DBNAME_DOCUMENT_STORE = "document_store";

    private SecondaryDatabase handleIndex;
    private SecondaryConfig handleIndexConfig;
    private final String DBNAME_HANDLE_INDEX = "handle_index";

    BerkeleyDBStore() {
        reinitialize();
    }

    public void reinitialize() {
        initializeEnvironment();
        initializeDatabases();
    }

    private void initializeEnvironment() {

        String berkeleyDir = System.getProperty("berkeleyDir");
        if(null == berkeleyDir) {
            berkeleyDir = PATH_BERKELEYDB_DIRECTORY;
        }
        else if(berkeleyDir.equals("RAM")) {
            properties.put(BerkeleyDBCoreConstants.LOG_MEM_ONLY, "true");
            Logger.info("Initializing BerkeleyDB in memory-only mode");
        }

        //TODO: These properties need to be enabled by default... but how do we disable for tests
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_CHECKPOINTER, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_CLEANER, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_EVICTOR, "false");
        properties.put(BerkeleyDBCoreConstants.ENV_RUN_IN_COMPRESSOR, "false");
        properties.put(BerkeleyDBCoreConstants.MAX_MEMORY_PERCENT, "30");
        Logger.info("Starting Lucene embedded database in testing mode. " +
                    "Background threads disabled.");
        environmentConfig = new EnvironmentConfig(properties);
        environmentConfig.setAllowCreate(true);
        Logger.info("BerkeleyDB cache authorized to use " + environmentConfig.getCachePercent() + "% of the JVM RAM");

        final File storeFile = new File(berkeleyDir);
        try {
            storeFile.mkdir();
            Logger.error("Lucene's BerkeleyDB initialized with directory: " + berkeleyDir);
        }
        catch(SecurityException e) {
            Logger.error("Security violation occurred while trying to create the embedded database directory.");
        }

        try {
            environment = new Environment(storeFile, environmentConfig);
        } catch (DatabaseException e) {
            Logger.error("Error occurred while trying to create the embedded database environment.");
        }
    }

    private void initializeDatabases() {

        catalogConfig = new DatabaseConfig();
        catalogConfig.setAllowCreate(true);
        try {
            catalogDatabase = environment.openDatabase(null, DBNAME_CATALOG, catalogConfig);
            storedClassCatalog = new StoredClassCatalog(catalogDatabase);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
        documentKeyBinding = new SerialBinding(storedClassCatalog, String.class);
        documentDataBinding = new SerialBinding(storedClassCatalog, EDBDocument.class);
        handleIndexKeyBinding = new SerialBinding(storedClassCatalog, String.class);

        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        try {
            documentStore = environment.openDatabase(null, DBNAME_DOCUMENT_STORE, databaseConfig);
        } catch (DatabaseException e) {
            Logger.error("Failed to access the requested database from the environment.");
        }

        handleIndexConfig = new SecondaryConfig();
        handleIndexConfig.setAllowCreate(true);
        handleIndexConfig.setSortedDuplicates(true);
        handleIndexConfig.setKeyCreator(new HandleIndexKeyCreator(documentKeyBinding, handleIndexKeyBinding));
        try {
            handleIndex = environment.openSecondaryDatabase(null, DBNAME_HANDLE_INDEX, documentStore, handleIndexConfig);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    public void put(final String documentKey, final EDBDocument document) {
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStore.put(null, entryKey, entryData);
        } catch (DatabaseException e) {
            Logger.error("Failed to insert entry into the document store.");
        }
    }

    public EDBDocument get(final String documentKey) {
        EDBDocument document = new EDBDocument();
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStore.get(null, entryKey, entryData, LockMode.DEFAULT);
            document = (EDBDocument) documentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            Logger.error("Failed to retrieve requested document from document store.");
        }
        return document;
    }

    public void purgeStaleHandle(final String staleHandle) {
        final DatabaseEntry entryKey = new DatabaseEntry();
        handleIndexKeyBinding.objectToEntry(staleHandle, entryKey);
        try {
            handleIndex.delete(null, entryKey);
        } catch (DatabaseException e) {
            Logger.error("Failed to delete entry from the handle index.");
        }
    }

    public void delete(final String documentKey) {
        final DatabaseEntry entryKey = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);

        try {
            documentStore.delete(null, entryKey);
        } catch (DatabaseException e) {
            Logger.error("Failed to delete entry from the document store.");
        }
    }

    public void close() {
        try {
            documentStore.close();
            environment.close();
            Logger.info("Releasing resources for embedded database environment.");
        } catch (DatabaseException e) {
            Logger.error("Failed to release resources for embedded database environment.");
        }
    }

    public void purgeAllDocuments() {
        try {
            documentStore.close();
            handleIndex.close();
            environment.truncateDatabase(null, DBNAME_DOCUMENT_STORE, true);
            reinitialize();
        } catch (DatabaseException e) {
            Logger.error("Failed to truncate the document store");
        }
    }

    Database getStore() {
        return documentStore;
    }

    Long totalDocumentStoreRowCount() {
        Long rowCount = null;
        try {
            rowCount = documentStore.count();
        } catch (DatabaseException e) {
            Logger.error("Unable to return database row count.");
        }
        return rowCount;
    }

    public void printBerkeleyInformation() {
        EnvironmentStats environmentStats = null;
        try {
            environmentStats = environment.getStats(StatsConfig.DEFAULT);
        } catch (DatabaseException e) {
            Logger.error("Failed to acquire environment statistics.");
        }
        Logger.info("Total cache size in bytes: " + environmentStats.getCacheTotalBytes());
        Logger.info("Total estimated log size: " + environmentStats.getTotalLogSize());
        Logger.info("Total rows in document store: " + totalDocumentStoreRowCount());
    }

}
