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

import java.io.Serializable;

/**
 * Created by rlmathes on 7/27/17.
 */
public class EDBDocumentKey implements Serializable {

    private String segmentName;
    private int documentID;

    public EDBDocumentKey(final String segmentName, final int documentID) {
        this.segmentName = segmentName;
        this.documentID = documentID;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public int getDocumentID() {
        return documentID;
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof EDBDocumentKey) {
            final EDBDocumentKey compareKey = (EDBDocumentKey) o;
            if(this.segmentName.equals(compareKey.getSegmentName()) && this.documentID == compareKey.getDocumentID()) {
                return true;
            }
        }
        return false;
    }

}
