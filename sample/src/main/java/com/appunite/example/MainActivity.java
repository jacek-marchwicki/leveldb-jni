/**
 * Copyright [2016] <jacek.marchwicki@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.appunite.example;

import android.app.Activity;
import android.os.Bundle;
import android.widget.TextView;

import com.appunite.leveldb.KeyNotFoundException;
import com.appunite.leveldb.LevelDB;
import com.appunite.leveldb.LevelDBException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;

public class MainActivity extends Activity {

    protected static final byte[] VALUE_KEY = "key".getBytes();

    public static class ByteUtils {
        private static ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);

        public static byte[] longToBytes(long x) {
            buffer.clear();
            buffer.putLong(0, x);
            return buffer.array();
        }

        public static long bytesToLong(byte[] bytes) {
            buffer.clear();
            buffer.put(bytes, 0, bytes.length);
            buffer.flip();//need flip
            return buffer.getLong();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        final TextView textView = (TextView) findViewById(R.id.activity_main_text);
        try {
            final File databasePath = getDatabasePath("database.leveldb");
            if (!databasePath.isDirectory() && !databasePath.mkdirs()) {
                throw new IOException("Could not create directory");
            }
            final LevelDB db = new LevelDB(databasePath.getAbsolutePath());
            final long value;
            if (db.exists(VALUE_KEY)) {
                value = ByteUtils.bytesToLong(db.getBytes(VALUE_KEY)) + 1L;
            } else {
                value = 0L;
            }
            db.putBytes(VALUE_KEY, ByteUtils.longToBytes(value));
            db.close();
            textView.setText(String.format(Locale.getDefault(), "Value: %d", value));
        } catch (LevelDBException | IOException | KeyNotFoundException e) {
            e.printStackTrace();
            textView.setText(String.format(Locale.getDefault(), "Database error: %s", e.getMessage()));
        }
    }

}
