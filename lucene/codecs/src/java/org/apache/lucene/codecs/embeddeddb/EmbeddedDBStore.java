package org.apache.lucene.codecs.embeddeddb;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public enum EmbeddedDBStore {

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private StoredClassCatalog storedClassCatalog;
    private final Properties properties = new Properties();
    private final String PATH_EMBEDDEDDB_STORE = "tmp_lucene_embedded_store_directory";
    private final String LOG_MEM_ONLY = "je.log.memOnly";
    private Map<String, List<DocumentKey>> keyStore = new HashMap<>();

    private Database documentStoreDatabase;
    private EntryBinding documentKeyBinding;
    private EntryBinding documentDataBinding;
    private EntryBinding keyStoreBinding;
    private EntryBinding keyStoreKeyBinding;
    private final String DBNAME_DOCUMENT_STORE = "document_store";

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
            documentStoreDatabase = environment.openDatabase(null, DBNAME_DOCUMENT_STORE, databaseConfig);
            storedClassCatalog = new StoredClassCatalog(documentStoreDatabase);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to access the requested database from the environment.");
        }

        documentKeyBinding = new SerialBinding(storedClassCatalog, DocumentKey.class);
        documentDataBinding = new SerialBinding(storedClassCatalog, DocumentData.class);
        keyStoreBinding = new SerialBinding(storedClassCatalog, Map.class);
        keyStoreKeyBinding = new SerialBinding(storedClassCatalog, String.class);
    }

    Database getStore() {
        return documentStoreDatabase;
    }



    public void put(final String segmentName, final int docID, final DocumentData document) {

        final DocumentKey documentKey = new DocumentKey(segmentName, docID);

        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);
        try {
            documentStoreDatabase.put(null, entryKey, entryData);
            if(keyStore.containsKey(segmentName)) {
                keyStore.get(segmentName).add(documentKey);
            }
            else {
                List<DocumentKey> keys = new ArrayList<>();
                keys.add(documentKey);
                keyStore.put(segmentName, keys);
            }

        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to insert entry into the document store.");
        }
    }

    public DocumentData get(final String segmentName, final int docID) {

        final DocumentKey documentKey = new DocumentKey(segmentName, docID);
        DocumentData document = new DocumentData();

        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(documentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            document = (DocumentData) documentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to retrieve requested document from document store.");
        }
        return document;
    }

    public Map<Integer, DocumentData> getDocumentsForSegment(final String segmentName) {

        Map<Integer, DocumentData> documentSet = new HashMap<>();

        for(DocumentKey key : keyStore.get(segmentName)) {
            documentSet.put(key.getDocumentID(), get(key.getSegmentName(), key.getDocumentID()));
        }

        return documentSet;
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
            environment.truncateDatabase(null, DBNAME_DOCUMENT_STORE, false);
            Logger.LOG(LogLevel.INFO, "Truncating the document store.");
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to truncate the document store");
        }
    }

    public void reinitialize() {
        initializeEnvironment();
        initializeDatabases();
    }

}
