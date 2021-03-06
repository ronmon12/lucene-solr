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
 *
 */
public class BerkeleyDBCoreConstants {

    static final String ENV_RUN_IN_COMPRESSOR = "je.env.runINCompressor";
    static final String ENV_RUN_CHECKPOINTER = "je.env.runCheckpointer";
    static final String ENV_RUN_CLEANER = "je.env.runCleaner";
    static final String ENV_RUN_EVICTOR = "je.env.runEvictor";
    static final String LOG_MEM_ONLY = "je.log.memOnly";
    static final String MAX_MEMORY_PERCENT = "je.maxMemoryPercent";
}
