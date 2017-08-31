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

import java.io.IOException;
import java.util.UUID;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Created by rlmathes on 7/15/17.
 */
public class EmbeddedDBStoredFieldsWriter extends StoredFieldsWriter {

    /** Extension of stored fields file */
    public static final String FIELDS_EXTENSION = "fdt";
    private String writerHandle;
    private EDBDocument currentDocument;
    private int documentID = 0;
    private Directory directory;
    private String segment;
    private IndexOutput fieldsStream;

    public EmbeddedDBStoredFieldsWriter(Directory directory, String segment, IOContext context) throws IOException {

        this.directory = directory;
        this.segment = segment;
        boolean success = false;
        try {
            fieldsStream = directory.createOutput(IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION), context);
            UUID writerUUID = UUID.randomUUID();
            fieldsStream.writeString(writerUUID.toString());
            fieldsStream.close();
            writerHandle = writerUUID.toString();
            success = true;
        } finally {
            if (!success) {
                abort();
            }
        }
    }

    @Override
    public void startDocument() throws IOException {
        currentDocument = new EDBDocument();
    }

    @Override
    public void finishDocument() throws IOException {
        StringBuilder documentKeyBuilder = new StringBuilder(writerHandle);
        documentKeyBuilder.append("_");
        documentKeyBuilder.append(documentID);
        BerkeleyDBStore.INSTANCE.put(documentKeyBuilder.toString(), currentDocument);
        documentID++;
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {

        EDBStoredField edbStoredField = new EDBStoredField();
        edbStoredField.setName(field.name());

        if(null != field.numericValue()) {
            edbStoredField.setNumericValue(field.numericValue());
        }
        else if(null != field.binaryValue()) {
            edbStoredField.setBinaryValue(field.binaryValue().bytes);
            edbStoredField.setOffset(field.binaryValue().offset);
            edbStoredField.setLength(field.binaryValue().length);
        }
        else if(null != field.stringValue()) {
            final BytesRef fieldBytesRef = new BytesRef(field.stringValue());
            final String utf8AdjustedField = new String(fieldBytesRef.bytes, fieldBytesRef.offset, fieldBytesRef.length);
            edbStoredField.setStringValue(utf8AdjustedField);
        }

        currentDocument.addField(edbStoredField);
    }

    @Override
    public void abort() {
        try {
            close();
        } catch (Throwable ignored) {}
        IOUtils.deleteFilesIgnoringExceptions(directory,
                IndexFileNames.segmentFileName(segment, "", FIELDS_EXTENSION));
    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(fieldsStream);
        } finally {
            fieldsStream = null;
        }
    }
}
