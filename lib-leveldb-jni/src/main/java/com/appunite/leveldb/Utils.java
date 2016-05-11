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

package com.appunite.leveldb;

import android.annotation.TargetApi;
import android.os.Build;

import java.nio.charset.Charset;

public class Utils {

    public static final Charset UTF_8 = Charset.forName("UTF-8");

    @TargetApi(Build.VERSION_CODES.GINGERBREAD)
    public static byte[] stringToBytes(String string) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.GINGERBREAD) {
            return string.getBytes(UTF_8);
        } else {
            return string.getBytes();
        }
    }

    @TargetApi(Build.VERSION_CODES.GINGERBREAD)
    public static String bytesToString(byte[] bytes) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.GINGERBREAD) {
            return new String(bytes, UTF_8);
        } else {
            return new String(bytes);
        }
    }
}
