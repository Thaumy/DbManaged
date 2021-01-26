plugins {
    java
    kotlin("jvm") version "1.4.21"
}

group = "thaumy.cn"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    testCompile("junit", "junit", "4.12")
}
