## Using

Two new program arguments are offered for Lucene:

| Program Argument                 | Description |
| -------------------------------- | ----------- |
| -DluceneEmbeddedDBMemOnly        | Start Lucene with the embedded DB in-memory only |
| -DluceneEmbeddedDBStoreDirectory | Specify an output directory for the embedded database, if not specified a default directory will be made |

## Todo List
[x] Clone lucene-solr project in my space and create new branch

[x] Clone branch to local and setup IDE environment

[x] Understand Lucene410Codec and how to interact/test it with mock data

[x] Write tests cases w/ some sort of test data for validation

[x] Extend FilterCodec with a dummy StoredFieldsFormat

[x] Research embedded databases, choose one for the new codec

[x] Design new codec... more details TBD

[x] Implement new codec, will need new writer and reader to go with StoredFieldsFormat... more details TBD

## Questions
