package org.apache.lucene.codecs.embeddeddb;

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

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

/**
 * Created by rlmathes on 8/11/17.
 */
class HandleIndexKeyCreator implements SecondaryKeyCreator {

    private EntryBinding documentKeyBinding;
    private EntryBinding handleIndexKeyBinding;

    public HandleIndexKeyCreator(EntryBinding documentKeyBinding, EntryBinding handleIndexKeyBinding) {
        this.documentKeyBinding = documentKeyBinding;
        this.handleIndexKeyBinding = handleIndexKeyBinding;
    }

    public boolean createSecondaryKey(SecondaryDatabase secondary,
                                      DatabaseEntry key,
                                      DatabaseEntry data,
                                      DatabaseEntry result) {

        String documentKey = (String) documentKeyBinding.entryToObject(key);
        StringBuilder secondaryKeyBuilder = new StringBuilder(documentKey);
        String handle = secondaryKeyBuilder.substring(0, secondaryKeyBuilder.indexOf("_"));
        handleIndexKeyBinding.objectToEntry(handle, result);
        return true;
    }
}


