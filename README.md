## Build and Testing

1. This project can be built using the following command; Maven
Ant must be installed and available in your path:

ant compile

2. To test the custom codec, navigate to the lucene directory and run the following ant command with VM arguments:

ant test -Dtests.codec=EmbeddedDB -Dtests.directory=EDBRAMDirectory

## Using

Two new program arguments are offered for Lucene:

| Program Argument                     | Description |
| ------------------------------------ | ----------- |
| -DluceneEmbeddedDBMemOnly (disabled) | This program argument is disabled, currently in-memory is always the mode|
| -DluceneEmbeddedDBStoreDirectory     | Specify an output directory for the embedded database, if not specified a default directory will be made |

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


## Test Failures

### At large
Test failures that need to be resolved.
- [ ] TestDisjunctionMaxQuery.testBooleanSpanQuery
- [ ] TestIndexWriterReader.testAddIndexes

### Sanctioned
Test failures that cannot be avoided due to a specific/particular
attribute of the test.
- TestIndexFileDeleter.testDeleteLeftoverFiles
