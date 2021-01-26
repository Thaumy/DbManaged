plugins {
    java
    kotlin("jvm") version "1.4.21"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    maven("https://mirrors.huaweicloud.com/repository/maven/")
    maven("https://jitpack.io")
    mavenCentral()
    jcenter()
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("mysql:mysql-connector-java:8.0.17")
    implementation(fileTree("src/main/resources/"))
    testCompile("junit", "junit", "4.12")
}
