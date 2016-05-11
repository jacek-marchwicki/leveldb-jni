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

import java.io.Closeable;

public class LevelIterator implements Closeable {
    @SuppressWarnings("unused")
    /**
     * We need use this field inside JNI
     */
    private final long nativePointer;

    LevelIterator(long nativePointer) {
        this.nativePointer = nativePointer;
    }

    @Override
    public void close() {
        nativeClose();
    }

    public void seekToFirst(byte[] startingKey) {
        nativeSeekToFirst(startingKey);
    }

    public void seekToFirst() {
        nativeSeekToFirst();
    }


    public boolean isValid() {
        return nativeIsValid();
    }

    public byte[] key() {
        return nativeKey();
    }


    public byte[] value() {
        return nativeValue();
    }

    public void next() {
        nativeNext();
    }

    private native void nativeClose();
    private native void nativeSeekToFirst();
    private native void nativeSeekToFirst(byte[] startingKey);
    private native boolean nativeIsValid();
    private native byte[] nativeKey();
    private native byte[] nativeValue();
    private native void nativeNext();

}
