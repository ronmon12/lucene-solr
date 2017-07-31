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

/**
 * Created by rlmathes on 7/29/17.
 */
public interface EmbeddedDBStore {

    /**
     * Method will store a document object in the key/value database, using the given segment name and document ID
     *
     * @param segmentName - Name of segment the stored document is associated with
     * @param document - The object representative of the lucene document being stored
     */
    void put(final String segmentName, final EDBDocument document);

    /**
     * Method will return the desired document object based on the given segment name and document ID
     *
     * @param segmentName - Name of segment the stored document is associated with
     * @param docID - The iterative ID of the document being stored
     * @return - Returns a document object
     */
    EDBDocument get(final String segmentName, final int docID);
}
