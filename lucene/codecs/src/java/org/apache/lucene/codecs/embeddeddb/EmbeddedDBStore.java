package org.apache.lucene.codecs.embeddeddb;

import java.io.File;

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
 */
public enum EmbeddedDBStore {

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private Database documentStoreDatabase;
    private StoredClassCatalog storedClassCatalog;
    private EntryBinding documentDataBinding;
    private EntryBinding documentKeyBinding;
    private final String PATH_EMBEDDEDDB_STORE = "/Users/rlmathes/_temp/luceneStore"; //TODO: Integrate w/ config
    private final String DBNAME_DOCUMENT_STORE = "doc_store";

    EmbeddedDBStore() {

        environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate(true);
        File storeFile = new File(PATH_EMBEDDEDDB_STORE);
        try {
            storeFile.mkdir();
            //LOGIT: directory created for the store
        }
        catch(SecurityException e) {
            //LOGIT: Security violation while trying to create the directory for embedded database
        }

        try {
            environment = new Environment(storeFile, environmentConfig);
        } catch (DatabaseException e) {
            //LOGIT: Error occurred creating the embedded database environment
        }

        initializeDatabases();
    }


    private void initializeDatabases() {
        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        try {
            documentStoreDatabase = environment.openDatabase(null, DBNAME_DOCUMENT_STORE, databaseConfig);
            storedClassCatalog = new StoredClassCatalog(documentStoreDatabase);
        } catch (DatabaseException e) {
            //LOGIT: Failed to access the requested database from the environment
        }

        documentKeyBinding = new SerialBinding(storedClassCatalog, DocumentKey.class);
        documentDataBinding = new SerialBinding(storedClassCatalog, DocumentData.class);
    }


    Database getStore() {
        return documentStoreDatabase;
    }


    public void put(final DocumentKey key, final DocumentData data) {
        DatabaseEntry entryKey = new DatabaseEntry();
        DatabaseEntry entryData = new DatabaseEntry();
        documentKeyBinding.objectToEntry(key, entryKey);
        documentDataBinding.objectToEntry(data, entryData);
        try {
            documentStoreDatabase.put(null, entryKey, entryData);
        } catch (DatabaseException e) {
            e.printStackTrace();
            //LOGIT
        }

    }


    public DocumentData get(final DocumentKey key) {

        DatabaseEntry entryKey = new DatabaseEntry();
        DatabaseEntry entryData = new DatabaseEntry();
        DocumentData data = new DocumentData();
        documentKeyBinding.objectToEntry(key, entryKey);
        documentDataBinding.objectToEntry(data, entryData);

        try {
            documentStoreDatabase.get(null, entryKey, entryData, LockMode.DEFAULT);
            data = (DocumentData) documentDataBinding.entryToObject(entryData);
        } catch (DatabaseException e) {
            //LOGIT
        }

        return data;
    }


}
