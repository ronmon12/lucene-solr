## Build and Testing

1. Ensure Ant is installed and available in your path

2. Build the project with the following command

`ant clean compile`

3. To test the EmbeddedDB codec, navigate to the Lucene directory and run the following ant command with VM arguments:

`ant test -Dtests.codec=EmbeddedDB -Dtests.directory=EDBDirectory`

## Using

Available VM arguments for EmbeddedDB codec

| Program Argument                     | Description |
| ------------------------------------ | ----------- |
| -DberkeleyDir    | Specify an output directory for the BerkeleyDB embedded database, if not specified a default directory will be made. If keyword "RAM" is specified, BerkeleyDB will run in-memory only |

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
None

## Test Failures
| Test                            | Reason      | Plans       |
| --------------------------------| ----------- | ----------- |
| TestFieldsReader.testExceptions | Test expected an I/O exception | Addressed - my fields come from Berkeley, not files, so an IO exception from Directory wouldn't actually be expected|
| TestIndexWriterReader.testAddIndexes | Copying indexes between directories| Addressed - segment names may not be as important as once thought, UUID takes center stage to fix this test|
| TestIndexWriterUnicode.testInvalidUTF16 | Need to look into how I'm storing various UTF's | Addressed - Now storing string fields in UTF-8 for consistency |
| TestIndexFileDeleter.testDeleteLeftoverFiles | Test relies on files| Addressed - Fixed |
| TestIndexWriterDelete.testErrorInDocsWriterAdd | I don't think this test applies to my codec | Addressed - Fixed |
| TestIndexWriterMerging.testLucene | Not codec agnostic | Ignore - sanctioned per Caleb |
| TestIndexWriterOnDiskFull.testImmediateDiskFull | Not codec agnostic | Addressed - Fixed |
| TestSizeBoundedForceMerge.testByteSizeLimit | This test depends on expected files created by codec | Addressed - test can be ignored |
| OutOfMemoryError's | Cause my in-memory only BerkeleyDB | Addressed - reinstated traditional mode as default, now that the keys are corrected |
| TestRAMDirectory.testRAMDirectory | Uses a buildIndex() test method, is this a test flaw or design flaw? | Addressed, altered read and write handles to persist beyond a Directory|

