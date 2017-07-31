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
public enum BerkeleyDBStore implements EmbeddedDBStore{

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private StoredClassCatalog storedClassCatalog;
    private final Properties properties = new Properties();
    private final String PATH_EMBEDDEDDB_STORE = "tmp_lucene_embedded_store_directory";

    private Map<String, List<EDBDocumentKey>> keyStore = new HashMap<>();

    private Database documentStoreDatabase;
    private EntryBinding documentKeyBinding;
    private EntryBinding documentDataBinding;
    private EntryBinding keyStoreBinding;
    private EntryBinding keyStoreKeyBinding;
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
            Logger.LOG(LogLevel.DEBUG, "Database initialized with a record count of: " + documentStoreDatabase.count());
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to access the requested database from the environment.");
        }

        documentKeyBinding = new SerialBinding(storedClassCatalog, EDBDocumentKey.class);
        documentDataBinding = new SerialBinding(storedClassCatalog, EDBDocument.class);
        keyStoreBinding = new SerialBinding(storedClassCatalog, Map.class);
        keyStoreKeyBinding = new SerialBinding(storedClassCatalog, String.class);
        loadKeyStore();
    }

    public void put(final String segmentName, final EDBDocument document) {

        int docID = 0;
        if(keyStore.containsKey(segmentName)) {
            docID = keyStore.get(segmentName).size();
        }
        final EDBDocumentKey edbDocumentKey = new EDBDocumentKey(segmentName, docID);
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(edbDocumentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStoreDatabase.put(null, entryKey, entryData);
            if(keyStore.containsKey(segmentName)) {
                keyStore.get(segmentName).add(edbDocumentKey);
            }
            else {
                List<EDBDocumentKey> keys = new ArrayList<>();
                keys.add(edbDocumentKey);
                keyStore.put(segmentName, keys);
            }

        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to insert entry into the document store.");
        }
    }

    public EDBDocument get(final String segmentName, final int docID) {

        final EDBDocumentKey edbDocumentKey = new EDBDocumentKey(segmentName, docID);
        EDBDocument document = new EDBDocument();
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(edbDocumentKey, entryKey);
        documentDataBinding.objectToEntry(document, entryData);

        try {
            documentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            document = (EDBDocument) documentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to retrieve requested document from document store.");
        }
        return document;
    }

    public Map<Integer, EDBDocument> getDocumentsForSegment(final String segmentName) {

        Map<Integer, EDBDocument> documentSet = new HashMap<>();

        for(EDBDocumentKey key : keyStore.get(segmentName)) {
            documentSet.put(key.getDocumentID(), get(key.getSegmentName(), key.getDocumentID()));
        }

        return documentSet;
    }

    private void persistKeyStore() {
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        keyStoreBinding.objectToEntry(keyStore, entryData);
        keyStoreKeyBinding.objectToEntry("keystore", entryKey);

        try {
            documentStoreDatabase.put(null, entryKey, entryData);
        }
        catch(DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to insert keystore into database.");
        }
    }

    private void loadKeyStore() {
        Map<String, List<EDBDocumentKey>> loadedKeyStore = new HashMap<>();
        final DatabaseEntry entryKey = new DatabaseEntry();
        final DatabaseEntry entryData = new DatabaseEntry();
        keyStoreBinding.objectToEntry(loadedKeyStore, entryData);
        keyStoreKeyBinding.objectToEntry("keystore", entryKey);

        try {
            documentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            loadedKeyStore = (Map<String, List<EDBDocumentKey>>) keyStoreBinding.entryToObject(entryData);
            this.keyStore = loadedKeyStore;
            if(loadedKeyStore.size() == 0) {
                Logger.LOG(LogLevel.INFO, "No existing keystore, creating new keystore for the database.");
            }
            else {
                Logger.LOG(LogLevel.INFO, "Loading existing keystore from the database.");
            }
        }
        catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to load keystore from the database.");
        }

    }

    public void printKeyStoreStats() {
        for(Map.Entry<String, List<EDBDocumentKey>> entry : keyStore.entrySet()) {
            System.out.println("Segment " + entry.getKey() + " contains " + entry.getValue().size() + " document keys");
        }
    }

    public void close() {
        /*
         * Currently persisting keystore only at database close; this does pose a risk if the database should
         * shut down unexpectedly, in which case the keystore would not match the database contents. Decide whether to
         * take a performance hit and persist the keystore every database.put, or have a disaster recovery
         * method "rebuildKeyStore" which would recreate the keystore from the contents in the database.
         *
         */
        persistKeyStore();

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
            Logger.LOG(LogLevel.INFO, "Truncating the document store.");
        } catch (DatabaseException e) {
            Logger.LOG(LogLevel.ERROR, "Failed to truncate the document store");
        }
    }


    Database getStore() {
        return documentStoreDatabase;
    }

    Map<String, List<EDBDocumentKey>> getKeyStore() {
        return keyStore;
    }
}
