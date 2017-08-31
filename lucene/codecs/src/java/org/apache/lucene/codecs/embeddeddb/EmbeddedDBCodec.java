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

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene410.Lucene410Codec;

/**
 * Created by rlmathes on 7/15/17.
 *
 * Codec which stores document fields within an embedded database
 *
 */
public class EmbeddedDBCodec extends FilterCodec {

    private final StoredFieldsFormat storedFields = new EmbeddedDBStoredFieldsFormat();

    public EmbeddedDBCodec() {
        super("EmbeddedDB", new Lucene410Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }
}
