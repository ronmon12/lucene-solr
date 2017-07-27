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
public enum EmbeddedDBStore {

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private Database segmentStoreDatabase;
    private StoredClassCatalog storedClassCatalog;
    private EntryBinding segmentDataBinding;
    private EntryBinding segmentKeyBinding;
    private final Properties properties = new Properties();
    private final String PATH_EMBEDDEDDB_STORE = "tmp_lucene_embedded_store_directory";
    private final String DBNAME_SEGMENT_STORE = "segment_store";
    private final String LOG_MEM_ONLY = "je.log.memOnly";

    EmbeddedDBStore() {
        initializeEnvironment();
        initializeDatabases();
    }

    private void initializeEnvironment() {
        String luceneEmbeddedDBMemOnly = System.getProperty("luceneEmbeddedDBMemOnly");
        if(luceneEmbeddedDBMemOnly != null && luceneEmbeddedDBMemOnly.equals("true")) {
            properties.put(LOG_MEM_ONLY, "true");
            Logger.LOG(LogLevel.INFO, "Starting Lucene embedded database in memory-only mode.");
        }
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
            segmentStoreDatabase = environment.openDatabase(null, DBNAME_SEGMENT_STORE, databaseConfig);
            storedClassCatalog = new StoredClassCatalog(segmentStoreDatabase);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to access the requested database from the environment.");
        }

        segmentKeyBinding = new SerialBinding(storedClassCatalog, String.class);
        segmentDataBinding = new SerialBinding(storedClassCatalog, SegmentData.class);
    }

    Database getStore() {
        return segmentStoreDatabase;
    }

    public void put(final String key, final SegmentData data) {
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        segmentKeyBinding.objectToEntry(key, entryKey);
        segmentDataBinding.objectToEntry(data, entryData);
        try {
            segmentStoreDatabase.put(null, entryKey, entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to insert entry into the segment store.");
        }
    }


    public SegmentData get(final String key) {

        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        SegmentData data = new SegmentData();
        segmentKeyBinding.objectToEntry(key, entryKey);
        segmentDataBinding.objectToEntry(data, entryData);

        try {
            segmentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            data = (SegmentData) segmentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to retrieve requested segment from segment store.");
        }
        return data;
    }

    public void close() {

        try {
            environment.close();
            Logger.LOG(LogLevel.INFO, "Releasing resources for embedded database environment.");
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to release resources for embedded database environment.");
        }
    }

    public void purge() {
        try {
            environment.truncateDatabase(null, DBNAME_SEGMENT_STORE, false);
            Logger.LOG(LogLevel.INFO, "Truncating the segment store.");
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to truncate the segment store");
        }
    }

    public void reinitialize() {
        initializeEnvironment();
        initializeDatabases();
    }


}
