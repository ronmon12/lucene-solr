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

import java.util.List;

import com.sleepycat.je.Database;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by rlmathes on 7/16/17.
 */
public class TestEmbeddedDBStore {

    @Test
    public void getStore() throws Exception {

        Database store = EmbeddedDBStore.INSTANCE.getStore();
        Assert.assertEquals("segment_store", store.getDatabaseName());
    }

    @Test
    public void put() {

        String key = "seg_1";
        SegmentData data = new SegmentData();
        EDBStoredDocument doc = new EDBStoredDocument();
        EDBStoredField field = new EDBStoredField();
        field.setStringValue("test_value");
        doc.addField(field);
        data.putDocument(0, doc);
        EmbeddedDBStore.INSTANCE.put(key, data);

        List<EDBStoredField> fields = EmbeddedDBStore.INSTANCE.get(key).getDocument(0).getFields();
        Assert.assertEquals("test_value", fields.get(0).getStringValue());
    }

    @Test
    public void getRAMUsed() {
        long ram = EmbeddedDBStore.INSTANCE.getRAMUsed();
        System.out.println(ram);
    }
}