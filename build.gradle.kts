import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.21"
    id("com.commercehub.gradle.plugin.avro") version "0.16.0"
    application
}

group = "com.riywo.ninja"
version = "1.0-SNAPSHOT"

application {
    mainClassName = "com.riywo.ninja.bptree.MainKt"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.ajalt:clikt:1.7.0")
    compile("org.apache.avro:avro:1.8.2")
    compile("org.jline:jline:3.10.0")

    testCompile("org.junit.jupiter:junit-jupiter-api:5.4.0")
    testCompile("org.junit.jupiter:junit-jupiter-params:5.4.0")
    testCompile("org.assertj:assertj-core:3.11.1")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.4.0")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<Test> {
    useJUnitPlatform()
    systemProperties = mapOf(
        "junit.jupiter.testinstance.lifecycle.default" to "per_class"
    )
}
