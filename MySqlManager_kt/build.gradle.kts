plugins {
    java
    kotlin("jvm") version "1.4.21"
}

group = "thaumy.cn"
version = "1.0"

repositories {
    maven("https://mirrors.huaweicloud.com/repository/maven/")
    maven("https://jitpack.io")
    mavenCentral()
    jcenter()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("mysql:mysql-connector-java:8.0.17")
    testCompile("junit", "junit", "4.12")
}
