package org.apache.lucene.store;

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


import org.apache.lucene.codecs.embeddeddb.BerkeleyDBStore;

/**
 * Created by rlmathes on 7/31/17.
 */
public class EDBRAMDirectory extends RAMDirectory {

    /** Closes the store to future operations, releasing associated memory. */
    @Override
    public void close() {
        isOpen = false;
        fileMap.clear();
        BerkeleyDBStore.INSTANCE.purge();
    }
}
