import com.google.protobuf.gradle.*

plugins {
    java
    id("org.springframework.boot") version "3.2.4"
    id("io.spring.dependency-management") version "1.1.4"
    id("idea")
    id ("com.google.protobuf") version "0.9.4"
}

group = "com.xodb"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.projectlombok:lombok")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    "developmentOnly"("org.springframework.boot:spring-boot-devtools")

    implementation("org.rocksdb:rocksdbjni:9.0.0")

    implementation("org.apache.arrow:arrow-vector:9.0.0")
    implementation("org.apache.arrow:arrow-memory-netty:9.0.0")

    testImplementation("org.springframework.boot:spring-boot-starter-test")

    implementation("com.google.protobuf:protobuf-java:3.6.1")
    implementation("io.grpc:grpc-stub:1.15.1")
    implementation("io.grpc:grpc-protobuf:1.15.1")

    if (JavaVersion.current().isJava9Compatible) {
        // Workaround for @javax.annotation.Generated
        // see: https://github.com/grpc/grpc-java/issues/3633
        implementation("javax.annotation:javax.annotation-api:1.3.1")
    }

}

tasks.withType<Test> {
    useJUnitPlatform()
}

protobuf {
    protoc {
        // The artifact spec for the Protobuf Compiler
        artifact = "com.google.protobuf:protoc:3.6.1:osx-x86_64"
    }
    plugins {
        // Optional: an artifact spec for a protoc plugin, with "grpc" as
        // the identifier, which can be referred to in the "plugins"
        // container of the "generateProtoTasks" closure.
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.15.1:osx-x86_64"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without
                // options. Note the braces cannot be omitted, otherwise the
                // plugin will not be added. This is because of the implicit way
                // NamedDomainObjectContainer binds the methods.
                id("grpc") { }
            }
        }
    }
}