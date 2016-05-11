package com.appunite.leveldb;

import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

import static com.google.common.truth.Truth.assert_;


@SuppressWarnings("TryFinallyCanBeTryWithResources")
@RunWith(AndroidJUnit4.class)
public class LevelDBTest {

    @After
    public void tearDown() throws Exception {
        removeDatabases();
    }

    @Before
    public void setUp() throws Exception {
        removeDatabases();
    }

    private void removeDatabases() {
        deleteRecursive(getDatabaseFile("db"));
        deleteRecursive(getDatabaseFile("db1"));
        deleteRecursive(getDatabaseFile("db2"));
        deleteRecursive(getDatabaseFile("db_close"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    void deleteRecursive(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory()) {
            for (File child : fileOrDirectory.listFiles()) {
                deleteRecursive(child);
            }
        }
        fileOrDirectory.delete();
    }

    @Test
    public void testOpenedDatabase_isNotNull() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            assert_().that(db).isNotNull();
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testPutOnClosedDb_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        db.close();

        db.putBytes(Utils.stringToBytes("a"), Utils.stringToBytes("b"));
    }

    @Test(expected = LevelDBException.class)
    public void testGetOnClosedDb_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        db.close();

        db.getBytes(Utils.stringToBytes("a"));
    }

    @Test(expected = LevelDBException.class)
    public void testDeleteOnClosedDb_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        db.close();

        db.delete(Utils.stringToBytes("a"));
    }

    @Test(expected = LevelDBException.class)
    public void testExistsOnClosedDb_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        db.close();

        db.exists(Utils.stringToBytes("a"));
    }

    @Test(expected = LevelDBException.class)
    public void testNewInteratorOnClosedDb_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        db.close();

        db.newInterator();
    }

    @Test
    public void testTwoOpenDatabases_areDifferent() throws Exception {
        final LevelDB db1 = new LevelDB(getDatabase("db1"));
        try {
            final LevelDB db2 = new LevelDB(getDatabase("db2"));
            try {
                db1.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("db1"));
                db2.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("db2"));

                assert_().that(db1.getBytes(Utils.stringToBytes("key"))).isEqualTo(Utils.stringToBytes("db1"));
                assert_().that(db2.getBytes(Utils.stringToBytes("key"))).isEqualTo(Utils.stringToBytes("db2"));

            } finally {
                db2.close();
            }
        } finally {
            db1.close();
        }
    }

    @Test
    public void testSave_canBeRead() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("value"));

            assert_().that(db.getBytes(Utils.stringToBytes("key"))).isEqualTo(Utils.stringToBytes("value"));

        } finally {
            db.close();
        }
    }

    @Test(expected = KeyNotFoundException.class)
    public void testReadNonExistingString_throwKeyNotFoundException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.getBytes(Utils.stringToBytes("non_existing_key"));
        } finally {
            db.close();
        }
    }

    @Test(expected = KeyNotFoundException.class)
    public void testReadNonExistingBytes_throwKeyNotFoundException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.getBytes(Utils.stringToBytes("non_existing_key"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testSaveManyValues_valuesAreReturned() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key_1"), Utils.stringToBytes("value_1"));
            db.putBytes(Utils.stringToBytes("key_2"), Utils.stringToBytes("value_2"));
            db.putBytes(Utils.stringToBytes("key_3"), Utils.stringToBytes("value_3"));

            assert_().that(db.getBytes(Utils.stringToBytes("key_1"))).isEqualTo(Utils.stringToBytes("value_1"));
            assert_().that(db.getBytes(Utils.stringToBytes("key_2"))).isEqualTo(Utils.stringToBytes("value_2"));
            assert_().that(db.getBytes(Utils.stringToBytes("key_3"))).isEqualTo(Utils.stringToBytes("value_3"));
        } finally {
            db.close();
        }
    }

    @Test
    public void testExistingValue_existIsTrue() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("value"));

            assert_().that(db.exists(Utils.stringToBytes("key"))).isTrue();

        } finally {
            db.close();
        }
    }

    @Test
    public void testCloseDatabaseTwice_nothingHappenWrong() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db_close"));
        db.close();
        db.close();
    }


    private String getDatabase(String databaseName) {
        return getDatabaseFile(databaseName).getAbsolutePath();
    }

    private File getDatabaseFile(String databaseName) {
        return InstrumentationRegistry.getTargetContext().getDatabasePath(databaseName);
    }

    @Test
    public void testNonExistingValue_existIsFalse() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("other_key"), Utils.stringToBytes("value"));

            assert_().that(db.exists(Utils.stringToBytes("key"))).isFalse();

        } finally {
            db.close();
        }
    }

    @Test
    public void testDeleteExistingKey_notExistAgain() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("value"));
            assert_().that(db.exists(Utils.stringToBytes("key"))).isTrue();

            db.delete(Utils.stringToBytes("key"));
            assert_().that(db.exists(Utils.stringToBytes("key"))).isFalse();

        } finally {
            db.close();
        }
    }

    @Test
    public void testAfterDestroyingDatabase_newDatabaseIsEmpty() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key"), Utils.stringToBytes("value"));
        } finally {
            db.close();
        }
        LevelDB.destroy(getDatabase("db"));

        final LevelDB dbAgain = new LevelDB(getDatabase("db"));
        try {
            assert_().that(dbAgain.exists(Utils.stringToBytes("key"))).isFalse();
        } finally {
            dbAgain.close();
        }
    }

    @Test
    public void testDeleteNonExistingKey_success() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.delete(Utils.stringToBytes("non_existing_key"));
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorWithoutCallingKeyWithoutSeek_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.key();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorWithoutCallingValueWithoutSeek_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.value();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testIteratorWithoutCallingIsValidWithoutSeek_returnFalse() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));
            final LevelIterator it = db.newInterator();
            try {
                assert_().that(it.isValid()).isFalse();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorWithoutCallingNextWithoutSeek_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.next();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testEmptyIterator_isValidIsFalse() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst();
                assert_().that(it.isValid()).isFalse();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testEmptyIterator_callingKeyTrowsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst();

                it.key();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorCallingNextAfterEnd_trowsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));

            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst();
                assert_().that(it.isValid()).isTrue();
                it.next();
                assert_().that(it.isValid()).isFalse();

                it.next();

            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testEmptyIterator_callingValueTrowsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst();

                it.value();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testIteratorStartWith_skipNotMatchingKeys1() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));
            db.putBytes(Utils.stringToBytes("key2"), Utils.stringToBytes("value2"));
            db.putBytes(Utils.stringToBytes("key3"), Utils.stringToBytes("value3"));
            db.putBytes(Utils.stringToBytes("key4"), Utils.stringToBytes("value4"));

            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst(Utils.stringToBytes("key2"));
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key2"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value2"));
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testIteratorStartWithNonExistingValue_isValidReturnsFalse() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));
            db.putBytes(Utils.stringToBytes("key2"), Utils.stringToBytes("value2"));
            db.putBytes(Utils.stringToBytes("key3"), Utils.stringToBytes("value3"));
            db.putBytes(Utils.stringToBytes("key4"), Utils.stringToBytes("value4"));

            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst(Utils.stringToBytes("key5"));
                assert_().that(it.isValid()).isFalse();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testIteratorStartWith_skipNotMatchingKeys2() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));
            db.putBytes(Utils.stringToBytes("key3"), Utils.stringToBytes("value3"));
            db.putBytes(Utils.stringToBytes("key4"), Utils.stringToBytes("value4"));

            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst(Utils.stringToBytes("key2"));
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key3"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value3"));
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test
    public void testIterator_returnAllValues() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            db.putBytes(Utils.stringToBytes("key1"), Utils.stringToBytes("value1"));
            db.putBytes(Utils.stringToBytes("key2"), Utils.stringToBytes("value2"));
            db.putBytes(Utils.stringToBytes("key3"), Utils.stringToBytes("value3"));
            db.putBytes(Utils.stringToBytes("key4"), Utils.stringToBytes("value4"));

            final LevelIterator it = db.newInterator();
            try {
                it.seekToFirst();
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key1"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value1"));

                it.next();
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key2"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value2"));


                it.next();
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key3"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value3"));


                it.next();
                assert_().that(it.isValid()).isTrue();
                assert_().that(it.key()). isEqualTo(Utils.stringToBytes("key4"));
                assert_().that(it.value()). isEqualTo(Utils.stringToBytes("value4"));

                it.next();
                assert_().that(it.isValid()).isFalse();
            } finally {
                it.close();
            }
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorNextOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.next();
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorIsValidOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.isValid();
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorSeekToFirstOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.seekToFirst();
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorSeekToFirstWithArgOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.seekToFirst(Utils.stringToBytes("a"));
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorKeyOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.key();
        } finally {
            db.close();
        }
    }

    @Test(expected = LevelDBException.class)
    public void testIteratorValueOnClosed_throwsException() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();

            it.value();
        } finally {
            db.close();
        }
    }

    @Test
    public void testIteratorCloseTwice_nothingWrong() throws Exception {
        final LevelDB db = new LevelDB(getDatabase("db"));
        try {
            final LevelIterator it = db.newInterator();
            it.close();
            it.close();
        } finally {
            db.close();
        }
    }
}
