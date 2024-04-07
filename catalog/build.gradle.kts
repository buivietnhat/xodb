plugins {
	java
	id("org.springframework.boot") version "3.2.4"
	id("io.spring.dependency-management") version "1.1.4"
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

//	implementation("org.apache.arrow:arrow-vector:5.0.0")
//	implementation("org.apache.arrow:arrow-memory:5.0.0")
//	implementation("org.apache.arrow:arrow-format:5.0.0")
//	implementation("org.apache.arrow:arrow-columnar:5.0.0")
//	implementation("org.apache.arrow:arrow-compute:5.0.0")
//	implementation("org.apache.arrow:arrow-flight:5.0.0")
//	implementation("org.apache.arrow:arrow-tools:5.0.0")
//	implementation("org.apache.arrow:arrow-jdbc:5.0.0")
//	implementation("org.apache.arrow:arrow-parquet:5.0.0")

	testImplementation("org.springframework.boot:spring-boot-starter-test")

}

tasks.withType<Test> {
	useJUnitPlatform()
}
