package org.apache.lucene.codecs.embeddeddb;

import java.io.File;
import java.nio.file.Files;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

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
public enum SegmentStore {

    INSTANCE;

    private EnvironmentConfig environmentConfig;
    private Environment environment;
    private DatabaseConfig databaseConfig;
    private Database segmentStoreDatabase;
    private StoredClassCatalog storedClassCatalog;
    private final String PATH_EMBEDDEDDB_STORE = "/Users/rlmathes/_temp/luceneStore"; //TODO: Integrate w/ config
    private final String DBNAME_SEGMENT_STORE = "segment_store";

    SegmentStore() {

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

        databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        try {
            segmentStoreDatabase = environment.openDatabase(null, DBNAME_SEGMENT_STORE, databaseConfig);
            storedClassCatalog = new StoredClassCatalog(segmentStoreDatabase);
        } catch (DatabaseException e) {
            //LOGIT: Failed to access the requested database from the environment
            e.printStackTrace();
        }
    }


    public Database getStore() {
        return segmentStoreDatabase;
    }







}
