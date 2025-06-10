/**
 * Copyright [2025] <jacek.marchwicki@gmail.com>
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
// */
plugins {
    alias(libs.plugins.artifactregistry)
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlin.android)
    id("maven-publish")
}

android {
    namespace = "com.appunite.leveldb.jni"
    compileSdk = libs.versions.sdk.compile.get().toInt()

    ndkVersion = libs.versions.sdk.ndk.get()
    defaultConfig {
        minSdk = libs.versions.sdk.min.get().toInt()

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
        externalNativeBuild {
            cmake {
                cppFlags("-Wall -Wno-shadow -Wconversion -Wno-unused-but-set-variable -Wno-deprecated-copy -Wno-implicit-int-float-conversion -Wno-shorten-64-to-32 -Wno-sign-conversion -Wno-implicit-int-conversion -Wno-unused-const-variable -Wno-error=deprecated-copy")
            }
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    externalNativeBuild {
        cmake {
            path("src/main/cpp/CMakeLists.txt")
            version = "3.22.1"
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
}

dependencies {

    implementation(libs.support.annotation)

    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.testing.espresso.core)
    androidTestImplementation(libs.testing.espresso.runner)
    androidTestImplementation(libs.testing.espresso.rules)
    androidTestImplementation(libs.testing.truth)
}


publishing {
    repositories {
        // Configure the repository you want to publish to.
        // For local testing:
        mavenLocal()
        maven {
            name = "GoogleArtifactRegistry"
            url = uri("artifactregistry://us-central1-maven.pkg.dev/gistr-app/maven-repo")
        }
    }
}

afterEvaluate {
    publishing {
        publications {
            create<MavenPublication>("release") {
                from(components["release"])

                groupId = "com.appunite"
                artifactId = "lib-leveldb-jni"
                version = "0.0.1-SNAPSHOT"

                pom {
                    name.set("lib-leveldb-jni")
                    description.set("Is very fast easy to use key-value database for Android")
                    url.set("https://github.com/jacek-marchwicki/leveldb-jni")

                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("jacek")
                            name.set("Jacek Marchwicki")
                            email.set("jacek.marchwicki@gmail.com")
                        }
                    }
                    scm {
                        connection.set("scm:git:https://github.com/jacek-marchwicki/leveldb-jni.git")
                        developerConnection.set("scm:git:https://github.com/jacek-marchwicki/leveldb-jni.git")
                        url.set("https://github.com/jacek-marchwicki/leveldb-jni")
                    }
                }
            }
        }

    }
}