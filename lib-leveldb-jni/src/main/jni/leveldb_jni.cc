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

#include "leveldb_jni.h"
#include <stdlib.h>
#include <sstream>

struct DatabaseNative {
    leveldb::DB* db;
};

struct IteratorNative {
    leveldb::Iterator* it;
};

struct WriteBatchNative {
    leveldb::WriteBatch* batch;
};

static const char *database_class_path_name = "com/appunite/leveldb/LevelDB";
static const char *iterator_class_path_name = "com/appunite/leveldb/LevelIterator";
static const char *write_batch_class_path_name = "com/appunite/leveldb/WriteBatch";
static const char *general_exception_class_path_name = "com/appunite/leveldb/LevelDBException";
static const char *key_not_found_exception_class_path_name = "com/appunite/leveldb/KeyNotFoundException";

static jfieldID database_clazz_field_native = NULL;
static jfieldID iterator_clazz_field_native = NULL;

void prepare_database_claszz_field_native(JNIEnv *env) {
    if (database_clazz_field_native == NULL) {
        jclass database_clazz = env->FindClass(database_class_path_name);
        database_clazz_field_native = env->GetFieldID(database_clazz, "nativeDB", "J");
    }
}

void prepare_iterator_claszz_field_native(JNIEnv *env) {
    if (iterator_clazz_field_native == NULL) {
        jclass iterator_clazz = env->FindClass(iterator_class_path_name);
        iterator_clazz_field_native = env->GetFieldID(iterator_clazz, "nativePointer", "J");
    }
}

void database_set_native(JNIEnv *env, jobject thiz, struct DatabaseNative *native_db) {
    prepare_database_claszz_field_native(env);
    env->SetLongField(thiz, database_clazz_field_native, (jlong) native_db);
}

struct DatabaseNative *database_get_native(JNIEnv *env, jobject thiz) {
    prepare_database_claszz_field_native(env);
    return (struct DatabaseNative * )env->GetLongField(thiz, database_clazz_field_native);
}

void iterator_set_native(JNIEnv *env, jobject thiz, struct IteratorNative *native_it) {
    prepare_iterator_claszz_field_native(env);
    env->SetLongField(thiz, iterator_clazz_field_native, (jlong) native_it);
}

struct IteratorNative *iterator_net_native(JNIEnv *env, jobject thiz) {
    prepare_iterator_claszz_field_native(env);
    return (struct IteratorNative * )env->GetLongField(thiz, iterator_clazz_field_native);
}


jint exception_throw(JNIEnv *env, const char *class_name, const char *msg) {
    jclass clazz = env->FindClass(class_name);
    if (!clazz) {
        LOGE("Can't find class %s", class_name);
        return JNI_ERR;
    }

    return env->ThrowNew(clazz, msg);
}

jint genearal_exception_throw(JNIEnv *env, const char *msg) {
    return exception_throw(env, general_exception_class_path_name, msg);
}

jint key_not_found_exception_throw(JNIEnv *env, const char *msg) {
    return exception_throw(env, key_not_found_exception_class_path_name, msg);
}

void database_release_if_not_released(JNIEnv *env, jobject thiz) {
    struct DatabaseNative * db = database_get_native(env, thiz);
    if (db != NULL) {
        delete db->db;
        free(db);
        database_set_native(env, thiz, NULL);
    }
}

void jni_database_open(JNIEnv *env, jobject thiz, jstring dbpath) {
    struct DatabaseNative *native = (struct DatabaseNative *)malloc(sizeof(struct DatabaseNative));
    if (native == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return;
    }
    memset(native, 0, sizeof(*native));

    const char *path = env->GetStringUTFChars(dbpath, 0);

    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kSnappyCompression;
    leveldb::Status status = leveldb::DB::Open(options, path, &native->db);

    if (!status.ok()) {
        database_release_if_not_released(env, thiz);
        env->ReleaseStringUTFChars(dbpath, path);
        free(native);
        genearal_exception_throw(env, status.ToString().c_str());
        return;
    } else {
        database_set_native(env, thiz, native);
        env->ReleaseStringUTFChars(dbpath, path);
    }
}

void jni_database_close(JNIEnv *env, jobject thiz) {
    database_release_if_not_released(env, thiz);
}

static void jni_database_destroy(JNIEnv *env,
                                 jclass clazz,
                                 jstring dbpath) {
    const char* path = env->GetStringUTFChars(dbpath, 0);

    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = DestroyDB(path, options);

    env->ReleaseStringUTFChars(dbpath, path);

    if (!status.ok()) {
        genearal_exception_throw(env, status.ToString().c_str());
    }
}

void jni_database_delete(JNIEnv *env, jobject thiz, jbyteArray jkey) {
    struct DatabaseNative *native = database_get_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
        return;
    }

    size_t key_len = (size_t) env->GetArrayLength(jkey);
    jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
    if (key_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
    } else {
        leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
        leveldb::Status status = native->db->Delete(leveldb::WriteOptions(), key);
        env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);

        if (!status.ok()) {
            std::string err("Failed to delete: " + status.ToString());
            genearal_exception_throw(env, err.c_str());
        }
    }
}


void jni_database_put_bytes(JNIEnv *env, jobject thiz, jbyteArray jkey, jbyteArray jvalue) {
    struct DatabaseNative *native = database_get_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
        return;
    }

    size_t value_len = (size_t) env->GetArrayLength(jvalue);
    jbyte* value_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jvalue, 0);

    if (value_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return;
    }

    size_t key_len = (size_t) env->GetArrayLength(jkey);
    jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
    if (key_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return;
    }

    leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
    leveldb::Slice value = leveldb::Slice(reinterpret_cast<char*>(value_bytes), value_len);

    leveldb::Status status = native->db->Put(leveldb::WriteOptions(), key, value);

    env->ReleasePrimitiveArrayCritical(jvalue, value_bytes, 0);
    env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);

    if (!status.ok()) {
        genearal_exception_throw(env, status.ToString().c_str());
    }

}

jbyteArray jni_database_get_bytes(JNIEnv *env, jobject thiz, jbyteArray jkey) {
    struct DatabaseNative *native = database_get_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
    } else {
        size_t key_len = (size_t) env->GetArrayLength(jkey);
        jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
        if (key_bytes == NULL) {
            genearal_exception_throw(env, "OutOfMemory");
        } else {
            leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
            std::string value;
            leveldb::Status status = native->db->Get(leveldb::ReadOptions(), key, &value);
            env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);

            if (status.ok()) {
                int size = value.size();
                char* elems = const_cast<char*>(value.data());
                jbyteArray array = env->NewByteArray(size * sizeof(jbyte));
                env->SetByteArrayRegion(array, 0, size, reinterpret_cast<jbyte*>(elems));
                return array;
            } else if (status.IsNotFound()) {
                key_not_found_exception_throw(env, "Not found");
            } else {
                genearal_exception_throw(env, status.ToString().c_str());
            }
        }
    }
}

jboolean jni_database_exists(JNIEnv *env, jobject thiz, jbyteArray jkey) {
    struct DatabaseNative *native = database_get_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
    } else {size_t key_len = (size_t) env->GetArrayLength(jkey);
        jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
        if (key_bytes == NULL) {
            genearal_exception_throw(env, "OutOfMemory");
        } else {
            leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
            std::string value;
            leveldb::Status status = native->db->Get(leveldb::ReadOptions(), key, &value);
            env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);

            if (status.ok()) {
                return JNI_TRUE;
            } else if (status.IsNotFound()) {
                return JNI_FALSE;
            } else {
                genearal_exception_throw(env, status.ToString().c_str());
            }
        }
    }
}

jobject jni_database_iterator(JNIEnv *env, jobject thiz) {
    struct DatabaseNative *native = database_get_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
    } else {
        leveldb::Iterator* it = native->db->NewIterator(leveldb::ReadOptions());
        if (it->status().ok()) {
            struct IteratorNative * nativeIt = (struct IteratorNative *)malloc(sizeof(struct IteratorNative));
            if (nativeIt == NULL) {
                delete it;
                genearal_exception_throw(env, "OutOfMemory");
            } else {
                memset(nativeIt, 0, sizeof(*nativeIt));
                nativeIt->it = it;

                jclass clazz = env->FindClass(iterator_class_path_name);
                jmethodID init = env->GetMethodID(clazz, "<init>", "(J)V");
                return env->NewObject(clazz, init, (jlong)nativeIt);
            }
        } else {
            genearal_exception_throw(env, it->status().ToString().c_str());
            delete it;
        }
    }
}

void jni_iterator_close(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native != NULL) {
        delete native->it;
        free(native);
        iterator_set_native(env, thiz, NULL);
    }
}

void jni_iterator_seek_to_first(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        native->it->SeekToFirst();
        if (!native->it->status().ok()) {
            genearal_exception_throw(env, native->it->status().ToString().c_str());
        }
    }
}

void jni_iterator_seek_to_first_with_key(JNIEnv *env, jobject thiz, jbyteArray jkey) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        size_t key_len = (size_t) env->GetArrayLength(jkey);
        jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
        if (key_bytes == NULL) {
            genearal_exception_throw(env, "OutOfMemory");
        } else {
            leveldb::Slice key = leveldb::Slice(reinterpret_cast<char *>(key_bytes), key_len);
            native->it->Seek(key);
            env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);

            if (!native->it->status().ok()) {
                genearal_exception_throw(env, native->it->status().ToString().c_str());
            }
        }
    }
}

jboolean jni_iterator_is_valid(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        bool valid = native->it->Valid();
        if (!native->it->status().ok()) {
            genearal_exception_throw(env, native->it->status().ToString().c_str());
        } else {
            return (jboolean)valid;
        }
    }
}

jbyteArray jni_iterator_key(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        if (!native->it->Valid() || !native->it->status().ok()) {
            genearal_exception_throw(env, "Cursor is not valid");
        } else {
            const leveldb::Slice &slice = native->it->key();
            if (!native->it->status().ok()) {
                genearal_exception_throw(env, native->it->status().ToString().c_str());
            } else {
                char *elems = const_cast<char *>(slice.data());
                jbyteArray array = env->NewByteArray(slice.size() * sizeof(jbyte));
                env->SetByteArrayRegion(array, 0, slice.size(), reinterpret_cast<jbyte *>(elems));
                return array;
            }
        }
    }
}

jbyteArray jni_iterator_value(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        if (!native->it->Valid() || !native->it->status().ok()) {
            genearal_exception_throw(env, "Cursor is not valid");
        } else {
            const leveldb::Slice &slice = native->it->value();
            if (!native->it->status().ok()) {
                genearal_exception_throw(env, native->it->status().ToString().c_str());
            } else {
                char* elems = const_cast<char*>(slice.data());
                jbyteArray array = env->NewByteArray(slice.size() * sizeof(jbyte));
                env->SetByteArrayRegion(array, 0, slice.size(), reinterpret_cast<jbyte *>(elems));
                return array;
            }
        }
    }
}

void jni_iterator_next(JNIEnv *env, jobject thiz) {
    struct IteratorNative *native = iterator_net_native(env, thiz);
    if (native == NULL) {
        genearal_exception_throw(env, "Cursor closed");
    } else {
        if (!native->it->Valid() || !native->it->status().ok()) {
            genearal_exception_throw(env, "Cursor is not valid");
        } else {
            native->it->Next();
            if (!native->it->status().ok()) {
                genearal_exception_throw(env, native->it->status().ToString().c_str());
            }
        }
    }
}

void jni_database_write(JNIEnv *env, jobject thiz, jlong nativePointer, jlong writeBatchNativePointer) {
    struct WriteBatchNative *nativeBatch = (WriteBatchNative *) writeBatchNativePointer;
    if (nativeBatch == NULL) {
        genearal_exception_throw(env, "WriteBatch is not open");
        return;
    }
    struct DatabaseNative *native = (DatabaseNative *) nativePointer;
    if (native == NULL) {
        genearal_exception_throw(env, "Database is not open");
        return;
    }

    leveldb::Status status = native->db->Write(leveldb::WriteOptions(), nativeBatch->batch);

    if (!status.ok()) {
        database_release_if_not_released(env, thiz);
        genearal_exception_throw(env, status.ToString().c_str());
        return;
    }
}

jlong jni_write_batch_create(JNIEnv *env, jobject thiz) {
    struct WriteBatchNative *native = (struct WriteBatchNative *)malloc(sizeof(struct WriteBatchNative));
    if (native == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return 0;
    }
    memset(native, 0, sizeof(*native));

    native->batch = new leveldb::WriteBatch;
    return (jlong)native;
}

void jni_write_batch_put_bytes(JNIEnv *env, jobject thiz, jlong nativePointer, jbyteArray jkey, jbyteArray jvalue) {
    struct WriteBatchNative *native = (WriteBatchNative *) nativePointer;
    if (native == NULL) {
        genearal_exception_throw(env, "WriteBatch is not open");
        return;
    }

    size_t value_len = (size_t) env->GetArrayLength(jvalue);
    jbyte* value_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jvalue, 0);

    if (value_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return;
    }

    size_t key_len = (size_t) env->GetArrayLength(jkey);
    jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
    if (key_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
        return;
    }

    leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
    leveldb::Slice value = leveldb::Slice(reinterpret_cast<char*>(value_bytes), value_len);

    native->batch->Put(key, value);

    env->ReleasePrimitiveArrayCritical(jvalue, value_bytes, 0);
    env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);
}

void jni_write_batch_delete(JNIEnv *env, jobject thiz, jlong nativePointer, jbyteArray jkey) {
    struct WriteBatchNative *native = (WriteBatchNative *) nativePointer;
    if (native == NULL) {
        genearal_exception_throw(env, "Batch is not open");
        return;
    }

    size_t key_len = (size_t) env->GetArrayLength(jkey);
    jbyte* key_bytes = (jbyte*)env->GetPrimitiveArrayCritical(jkey, 0);
    if (key_bytes == NULL) {
        genearal_exception_throw(env, "OutOfMemory");
    } else {
        leveldb::Slice key = leveldb::Slice(reinterpret_cast<char*>(key_bytes), key_len);
        native->batch->Delete(key);
        env->ReleasePrimitiveArrayCritical(jkey, key_bytes, 0);
    }
}

void jni_write_batch_clear(JNIEnv *env, jobject thiz, jlong nativePointer) {
    struct WriteBatchNative *native = (WriteBatchNative *) nativePointer;
    if (native == NULL) {
        genearal_exception_throw(env, "Batch is not open");
        return;
    }
    native->batch->Clear();
}

void jni_write_batch_free(JNIEnv *env, jobject thiz, jlong nativePointer) {
    struct WriteBatchNative *native = (WriteBatchNative *) nativePointer;
    if (native != NULL) {
        free(native);
    }
}

static JNINativeMethod database_methods[] = {
    { "nativeOpen", "(Ljava/lang/String;)V", (void*) jni_database_open},
    { "nativeClose", "()V", (void*) jni_database_close},
    { "nativeGetBytes", "([B)[B", (void*) jni_database_get_bytes},
    { "nativePutBytes", "([B[B)V", (void*) jni_database_put_bytes},
    { "nativeDelete", "([B)V", (void*) jni_database_delete},
    { "nativeExists", "([B)Z", (void*) jni_database_exists},
    { "nativeIterator", "()Lcom/appunite/leveldb/LevelIterator;", (void*) jni_database_iterator},
    { "nativeWrite", "(JJ)V", (void*) jni_database_write},
    { "nativeDestroy", "(Ljava/lang/String;)V", (void*) jni_database_destroy}
};

static JNINativeMethod iterator_methods[] = {
        { "nativeClose", "()V", (void*) jni_iterator_close},
        { "nativeSeekToFirst", "()V", (void*) jni_iterator_seek_to_first},
        { "nativeSeekToFirst", "([B)V", (void*) jni_iterator_seek_to_first_with_key},
        { "nativeIsValid", "()Z", (void*) jni_iterator_is_valid},
        { "nativeKey", "()[B", (void*) jni_iterator_key},
        { "nativeValue", "()[B", (void*) jni_iterator_value},
        { "nativeNext", "()V", (void*) jni_iterator_next},
};

static JNINativeMethod write_batch_methods[] = {
        { "nativeCreate", "()J", (void*) jni_write_batch_create},
        { "nativePutBytes", "(J[B[B)V", (void*) jni_write_batch_put_bytes},
        { "nativeDelete", "(J[B)V", (void*) jni_write_batch_delete},
        { "nativeClear", "(J)V", (void*) jni_write_batch_clear},
        { "nativeFree", "(J)V", (void*) jni_write_batch_free},
};


# define NELEM(x) ((int) (sizeof(x) / sizeof((x)[0])))

int JNI_OnLoad(JavaVM* vm, void *reserved) {
    JNIEnv* env = NULL;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6) != JNI_OK) {
        return JNI_ERR;
    };

    jclass database_class = env->FindClass(database_class_path_name);
    if (!database_class) {
        LOGE("Can't find class %s", database_class_path_name);
        return JNI_ERR;
    }

    if (env->RegisterNatives(database_class, database_methods, NELEM(database_methods))) {
        return JNI_ERR;
    }

    jclass iterator_class = env->FindClass(iterator_class_path_name);
    if (!iterator_class) {
        LOGE("Can't find class %s", iterator_class_path_name);
        return JNI_ERR;
    }

    if (env->RegisterNatives(iterator_class, iterator_methods, NELEM(iterator_methods))) {
        return JNI_ERR;
    }

    jclass write_batch_class = env->FindClass(write_batch_class_path_name);
    if (!write_batch_class) {
        LOGE("Can't find class %s", write_batch_class_path_name);
        return JNI_ERR;
    }

    if (env->RegisterNatives(write_batch_class, write_batch_methods, NELEM(write_batch_methods))) {
        return JNI_ERR;
    }

    return JNI_VERSION_1_6;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
}


