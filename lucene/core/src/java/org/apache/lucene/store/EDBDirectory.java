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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.embeddeddb.BerkeleyDBStore;

/**
 * Created by rlmathes on 8/11/17.
 *
 * EDBDirectory ensures that as stored field files age and are deleted, their associated rows in the Embedded Database (Berkeley)
 * are also deleted.
 */
public class EDBDirectory extends RAMDirectory {

    private List<String> handleTombstones = new ArrayList<>();
    public static final String FIELDS_EXTENSION = "fdt";

    /** Removes an existing file in the directory.
     * @throws IOException if the file does not exist
     */
    @Override
    public void deleteFile(String name) throws IOException {
        if(name.contains(FIELDS_EXTENSION)) {
            IndexInput fieldsStream = this.openInput(name, IOContext.DEFAULT);
            final String staleHandle = fieldsStream.readString();
            fieldsStream.close();
            handleTombstones.add(staleHandle);
        }
        super.deleteFile(name);
    }

    /** Closes the store to future operations, releasing associated memory. */
    @Override
    public void close() {
        for(final String staleHandle : handleTombstones) {
            BerkeleyDBStore.INSTANCE.purgeStaleHandle(staleHandle);
        }
        super.close();
    }
}
