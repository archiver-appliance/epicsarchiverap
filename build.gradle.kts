import com.diffplug.gradle.spotless.SpotlessExtension
import com.palantir.gradle.gitversion.VersionDetails
import org.apache.tools.ant.taskdefs.condition.Os
import java.io.FileOutputStream

plugins {
	`java-library`
	war
	eclipse
	alias(libs.plugins.analyze) // Analyzes dependencies for usage
	alias(libs.plugins.git.version)
	alias(libs.plugins.spotless)
	alias(libs.plugins.protobuf)
}

// =================================================================
// Project Configuration
// =================================================================

sourceSets {
	main {
		java {
			srcDir("src/main/")
		}
		proto {
			srcDir("src/proto")
		}
		resources {
			setSrcDirs(emptyList<String>())
		}
	}
	test {
		java {
			srcDir("src/test/")
		}
		resources {
			setSrcDirs(emptyList<String>())
		}
	}
}

java {
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(21))
	}
}

val runTestsSequentially: Boolean by extra {
	(findProperty("ARCHAPPL_SEQUENTIAL_TESTS") as? String)?.toBoolean() ?: false
}

val gitVersion: groovy.lang.Closure<String> by extra
val versionDetails: groovy.lang.Closure<VersionDetails> by extra
var gitWorks = true

try {
	version = gitVersion()

} catch (e: Throwable) {
	gitWorks = false
	if (project.hasProperty("projVersion")) {
		version = project.property("projVersion") as String
	} else {
		version = "unknown"
		logger.error("Failed to get git version: $e")
	}
}

val stageDir = layout.buildDirectory.file("stage").get()
val srcDir = layout.projectDirectory.file("src/main")
val libDir = layout.projectDirectory.file("lib")
val apiDocsDir = layout.projectDirectory.file("docs/api")
val archapplsite = System.getenv("ARCHAPPL_SITEID") ?: "tests"
val defaultsitespecificpath = "src/sitespecific/$archapplsite"
// Allow the site to be outside the repository
val sitespecificpath =
	if (file(defaultsitespecificpath).exists()) defaultsitespecificpath else archapplsite

ant.properties["stage"] = stageDir
ant.properties["archapplsite"] = archapplsite
ant.properties["sitespecificpath"] = sitespecificpath
ant.importBuild("build.xml")

// =================================================================
// Repositories & Dependencies
// =================================================================

repositories {
	mavenCentral()
	flatDir {
		name = "libs dir"
		dir("lib")
	}
	flatDir {
		name = "test libs dir"
		dir("lib/test")
	}
	maven { url = uri("https://clojars.org/repo") }
	ivy {
		url = uri("https://github.com/")
		patternLayout { artifact("/[organisation]/[module]/archive/[revision].[ext]") }
		metadataSources { artifact() }
	}
}

val viewer: Configuration by configurations.creating

dependencies {
	// Local JARs
	implementation(files("lib/jamtio_071005.jar", "lib/redisnio_0.0.1.jar"))

	// GitHub dependency via Ivy
	viewer("archiver-appliance:svg_viewer:${libs.versions.svg.viewer.get()}@zip")

	// Provided by Servlet Container (e.g., Tomcat)
	implementation(libs.tomcat.servlet.api)

	// Core Libraries
	implementation(libs.jca)
	implementation(libs.guava)
	implementation(libs.hazelcast)
	implementation(libs.jedis)
	implementation(libs.jython)
	implementation(libs.protobuf.java)
	implementation(libs.core.pva)
	implementation(libs.jakarta.validation)

	// Apache Commons
	implementation(libs.commons.codec)
	implementation(libs.commons.fileupload2.jakarta)
	implementation(libs.commons.fileupload2.core)
	implementation(libs.commons.io)
	implementation(libs.commons.lang3)
	implementation(libs.commons.math3)
	implementation(libs.commons.validator)


	// HTTP Clients
	implementation(libs.httpclient)
	implementation(libs.httpcore)

	// Data Formats & DB
	implementation(libs.jdbm) // clojar dependency
	implementation(libs.json.simple)
	implementation(libs.json)
	implementation(libs.opencsv)
	runtimeOnly(libs.mariadb)

	// Logging
	runtimeOnly(libs.log4j.one.two.api) // TODO remove log4j 1 from dependencies
	implementation(libs.log4j.api)
	implementation(libs.log4j.slf4j2)
	"permitUnusedDeclared"(libs.log4j.slf4j2)
	implementation(libs.log4j.jul)
	"permitUnusedDeclared"(libs.log4j.jul)
	runtimeOnly(libs.log4j.core)
	runtimeOnly(libs.disruptor) // Needed for async logging

	// Parquet support
	implementation(libs.parquet.protobuf)
	implementation(libs.parquet.column)
	implementation(libs.parquet.common)
	implementation(libs.parquet.hadoop)
	implementation(libs.hadoop.common)
	runtimeOnly(libs.hadoop.client)

	// Testing
	testImplementation(libs.junit.jupiter.api)
	testImplementation(libs.junit.jupiter.params)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
	testImplementation(libs.awaitility)
	testImplementation(libs.commons.compress)
	testImplementation(libs.commons.cli)
	testImplementation(libs.jinjava)
	testImplementation(files("lib/test/BPLTaglets.jar"))
	testImplementation(":pbrawclient:0.2.2")
	testImplementation(libs.tomcat.servlet.api)
	testImplementation(libs.mockito)
}

// =================================================================
// Documentation & Staging Tasks
// =================================================================

tasks.register<Delete>("cleanApiDocs") {
	group = "Clean"
	description = "Remove the generated java api docs."
	delete(apiDocsDir)
}

tasks.named("clean") { dependsOn("cleanApiDocs") }

tasks.register<JavaExec>("generateBPLActionsMappings") {
	group = "Documentation"
	description = "Generates the rest api documentation."
	outputs.file("$apiDocsDir/mgmtpathmappings.txt")
	doFirst {
		apiDocsDir.asFile.mkdirs()
		standardOutput = FileOutputStream("$apiDocsDir/mgmtpathmappings.txt")
	}
	mainClass.set("org.epics.archiverappliance.mgmt.BPLServlet")
	classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("generateJavaDocTagletScriptables") {
	group = "Documentation"
	description = "Generates the rest api documentation."
	dependsOn("generateBPLActionsMappings")
	mainClass.set("org.epics.archiverappliance.common.taglets.ProcessMgmtScriptables")
	inputs.file("$apiDocsDir/mgmt_scriptables.txt")
	outputs.file("$apiDocsDir/mgmt_scriptables.html")
	classpath = sourceSets.main.get().runtimeClasspath
	workingDir = project.projectDir
}

tasks.withType<Javadoc>().configureEach {
	dependsOn("generateBPLActionsMappings")
	mustRunAfter("generateBPLActionsMappings")
	finalizedBy("generateJavaDocTagletScriptables")
	doFirst {
		copy {
			from(
				layout.projectDirectory.file("LICENSE"),
				srcDir.asFile.resolve("edu/stanford/slac/archiverappliance/PB/EPICSEvent.proto")
			)
			into(layout.projectDirectory.dir("docs"))
		}
	}
	outputs.file("$apiDocsDir/mgmt_scriptables.txt")
	source = sourceSets.main.get().allJava

	options {
		require(this is StandardJavadocDocletOptions) // workaround https://github.com/gradle/gradle/issues/7038
		author(true)
		version(true)
		isUse = true
		links("https://docs.oracle.com/en/java/javase/12/docs/api/")
		taglets(
			"org.epics.archiverappliance.taglets.BPLActionTaglet",
			"org.epics.archiverappliance.taglets.BPLActionParamTaglet",
			"org.epics.archiverappliance.taglets.BPLActionEndTaglet"
		)
		tagletPath(file("lib/test/BPLTaglets.jar"))
	}
}

tasks.register<JavaExec>("syncStaticContentHeaderFooters") {
	group = "Staging"
	description = "Sync the headers and footers"
	dependsOn("compileJava")
	mainClass.set("org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters")
	args(
		"${srcDir}/org/epics/archiverappliance/mgmt/staticcontent/index.html",
		"${srcDir}/org/epics/archiverappliance/mgmt/staticcontent/"
	)
	classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<Zip>("stageSvgViewer") {
	group = "Staging"
	description = "Copy svg viewer project for assembling"

	val archPath = viewer.singleFile

	from(zipTree(archPath)) {
		include("svg_viewer-*/**")

		eachFile {
			val newPath = RelativePath(true, *relativePath.segments.drop(1).toTypedArray())
			relativePath = newPath
		}
	}

	archiveFileName.set("viewer.zip")
	includeEmptyDirs = false
	destinationDirectory.set(stageDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
}

tasks.register("sitespecificantscript") {
	group = "Staging"
	description = "Do the site specific changes from the ant script."
	dependsOn("stage")
	ant.properties["classes"] = sourceSets.main.get().runtimeClasspath.asPath
	finalizedBy("sitespecificbuild")
}

tasks.register("stage") {
	group = "Staging"
	description = "Copy static content from each of the projects into the staging directory."
	dependsOn(tasks.javadoc)
	finalizedBy("stageSvgViewer")
	doFirst { stageDir.asFile.mkdirs() }

	copy {
		from(project.projectDir.resolve("docs")) { include("*.*") }
		into(apiDocsDir)
	}
	copy {
		from(srcDir.asFile.resolve("org/epics/archiverappliance/staticcontent"))
		into(stageDir.asFile.resolve("org/epics/archiverappliance/staticcontent"))
	}
	copy {
		from(srcDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
		into(stageDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
	}
	copy {
		from(srcDir.asFile.resolve("org/epics/archiverappliance/mgmt/staticcontent"))
		into(stageDir.asFile.resolve("org/epics/archiverappliance/mgmt/staticcontent"))
	}
	doLast {
		stageDir
			.asFile.resolve("org/epics/archiverappliance/staticcontent/version.txt")
			.writeText("Archiver Appliance Version $version")
	}
}

tasks.register<Exec>("generateReleaseNotes") {
	group = "Staging"
	description = "Generate the Release Notes."
	outputs.file("$stageDir/RELEASE_NOTES")
	doFirst { standardOutput = FileOutputStream("$stageDir/RELEASE_NOTES") }
	commandLine("git", "log", "--oneline", "HEAD")
	isIgnoreExitValue = true
}

tasks.register<Exec>("sphinx") {
	group = "Staging"
	description = "Generate the documentation site."
	dependsOn(tasks.javadoc)
	workingDir = project.projectDir.resolve("docs")
	outputs.dir("docs/build")
	if (Os.isFamily(Os.FAMILY_WINDOWS)) {
		commandLine("cmd", "/c", "build_docs.bat")
	} else {
		commandLine("./build_docs.sh")
	}
}

// =================================================================
// Precompile steps
// =================================================================

protobuf {
	// Configure the protoc executable
	protoc {
		// Download from repositories
		artifact = "com.google.protobuf:protoc:${libs.versions.protobufJava.get()}"
	}
}

// =================================================================
// Artifact Assembly (WARs & Release)
// =================================================================

tasks.withType<War>().configureEach {
	dependsOn("stage", "sitespecificantscript")
	from(stageDir.asFile.resolve("org/epics/archiverappliance/staticcontent")) { into("ui/comm") }
	if (archapplsite == "tests") {
		from("src/resources/test/log4j2.xml") { into("WEB-INF/classes") }
	}
	from(sitespecificpath.let(::file).resolve("classpathfiles")) { into("WEB-INF/classes") }
	rootSpec.exclude("**/tomcat-servlet-api*.jar")
}

tasks.register<War>("mgmtWar") {
	group = "Wars"
	description = "Builds the mgmt war."
	dependsOn("sphinx")
	from(stageDir.asFile.resolve("org/epics/archiverappliance/mgmt/staticcontent")) { into("ui") }
	from(srcDir.asFile.resolve("org/epics/archiverappliance/config/persistence")) {
		include("*.sql")
		into("install")
	}
	from(project.projectDir.resolve("docs/docs/build")) { into("ui/help") }
	from(project.projectDir.resolve("docs/docs/source/samples")) {
		include("deployMultipleTomcats.py")
		into("install")
	}
	from(srcDir.asFile.resolve("org/epics/archiverappliance/common/scripts")) {
		filePermissions {
			user {
				read = true
				write = true
				execute = true
			}
			group {
				read = true
				execute = true
			}
			other {
				read = true
				execute = true
			}
		}
		into("install/pbutils")
	}

	archiveFileName.set("mgmt.war")
	webXml = srcDir.asFile.resolve("org/epics/archiverappliance/mgmt/WEB-INF/web.xml")
}

tasks.register<War>("etlWar") {
	group = "Wars"
	description = "Build the ELT war"
	archiveFileName.set("etl.war")
	webXml = srcDir.asFile.resolve("org/epics/archiverappliance/etl/WEB-INF/web.xml")
}

tasks.register<War>("engineWar") {
	group = "Wars"
	description = "Build the engine war"
	from(libDir.asFile.resolve("native/linux-x86")) {
		filePermissions {
			user {
				read = true
				write = true
				execute = true
			}
			group {
				read = true
				execute = true
			}
			other {
				read = true
				execute = true
			}
		}
		include("caRepeater")
		into("WEB-INF/lib/native/linux-x86")
	}
	from(libDir.asFile.resolve("native/linux-x86_64")) {
		filePermissions {
			user {
				read = true
				write = true
				execute = true
			}
			group {
				read = true
				execute = true
			}
			other {
				read = true
				execute = true
			}
		}
		include("caRepeater")
		into("WEB-INF/lib/native/linux-x86_64")
	}
	from(libDir.asFile.resolve("native")) {
		filePermissions {
			user {
				read = true
				write = true
				execute = true
			}
			group {
				read = true
				execute = true
			}
			other {
				read = true
				execute = true
			}
		}
		include("**/*.so")
		into("WEB-INF/lib/native/linux-x86")
	}

	archiveFileName.set("engine.war")
	webXml = srcDir.asFile.resolve("org/epics/archiverappliance/engine/WEB-INF/web.xml")
}

tasks.register<War>("retrievalWar") { // Corrected typo from retreivalWar
	group = "Wars"
	description = "Build the retrieval war"
	dependsOn("stageSvgViewer")
	from(stageDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent")) { into("ui") }

	archiveFileName.set("retrieval.war")
	webXml = srcDir.asFile.resolve("org/epics/archiverappliance/retrieval/WEB-INF/web.xml")
}

tasks.register<Tar>("buildRelease") {
	group = "Wars"
	description = "Builds a full release and zips up in a tar file."
	dependsOn("mgmtWar", "retrievalWar", "etlWar", "engineWar", "generateReleaseNotes")

	archiveFileName.set("archappl_v${version}.tar.gz")
	compression = Compression.GZIP
	from(layout.buildDirectory.file("libs/mgmt.war"))
	from(layout.buildDirectory.file("libs/engine.war"))
	from(layout.buildDirectory.file("libs/etl.war"))
	from(layout.buildDirectory.file("libs/retrieval.war"))
	from(project.projectDir) {
		include("LICENSE", "NOTICE", "*License.txt", "RELEASE_NOTES")
	}
	val samplesFolder = "docs/docs/source/samples"
	from(samplesFolder) {
		filePermissions {
			user {
				read = true
				write = true
				execute = true
			}
			group {
				read = true
				execute = true
			}
			other {
				read = true
				execute = true
			}
		}
		include("quickstart.sh")
	}
	from(samplesFolder) {
		include(
			"sampleStartup.sh",
			"deployMultipleTomcats.py",
			"addMysqlConnPool.py",
			"single_machine_install.sh"
		)
		into("install_scripts")
	}
	from("$samplesFolder/site_specific_content") {
		into("sample_site_specific_content")
	}
}

// =================================================================
// Test Task Definitions
// =================================================================

tasks.withType<Test>().configureEach {
	testClassesDirs = sourceSets.test.get().output.classesDirs
	classpath = sourceSets.test.get().runtimeClasspath

	maxParallelForks = if (runTestsSequentially) {
		1
	} else {
		(Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
	}

	doFirst {
		temporaryDir.resolve("sts").mkdirs()
		temporaryDir.resolve("mts").mkdirs()
		temporaryDir.resolve("lts").mkdirs()
		logger.lifecycle("Running tests with maxParallelForks = {}", maxParallelForks)
	}

	filter {
		includeTestsMatching("*Test")
	}

	maxHeapSize = "1G"
	jvmArgs = (listOf(
		"-Dlog4j1.compatibility=true"
	))

	environment("ARCHAPPL_SHORT_TERM_FOLDER", temporaryDir.resolve("sts").path)
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", temporaryDir.resolve("mts").path)
	environment("ARCHAPPL_LONG_TERM_FOLDER", temporaryDir.resolve("lts").path)

	doLast {
		delete(
			temporaryDir.resolve("sts"),
			temporaryDir.resolve("mts"),
			temporaryDir.resolve("lts")
		)
	}
}

tasks.named<ProcessResources>("processTestResources") {
	from(layout.projectDirectory.file("src/sitespecific/tests/classpathfiles"))
	from(layout.projectDirectory.file("src/resources/test")) {
		include("log4j2.xml")
		include("appliances.xml.j2")
		include("log4j2.component.properties")
	}
}

tasks.register<Test>("unitTests") {
	group = "Test"
	description = "Run all unit tests."
	useJUnitPlatform {
		excludeTags("flaky", "integration", "localEpics")
	}
}

tasks.register<Test>("flakyTests") {
	group = "Test"
	description = "Run unit tests that due to timing or machine specifics, could fail."
	useJUnitPlatform {
		includeTags("flaky")
		excludeTags("slow", "integration", "localEpics")
	}
}

tasks.register<Exec>("shutdownAllTomcats") {
	group = "Test"
	description = "Task to shut down all tomcats after running integration tests, if they didn't shut down correctly."
	setIgnoreExitValue(true)
	// pkill is not available on Windows, so only run this on non-Windows systems.
	if (!Os.isFamily(Os.FAMILY_WINDOWS)) {
		commandLine("pkill", "-9", "-f", "Deaatag=eaatesttm")
	} else {
		doFirst {
			logger.warn("pkill for tomcat shutdown is not supported on Windows. Skipping.")
		}
	}
}

tasks.register("integrationTestSetup") {
	group = "Test"
	description = "Setup for Integration Tests by backing up Tomcat's conf directory."
	dependsOn("buildRelease")

	val tomcatHome = System.getenv("TOMCAT_HOME")
	// Only configure and run this task if TOMCAT_HOME is set.
	onlyIf {
		if (tomcatHome == null) {
			logger.warn("TOMCAT_HOME environment variable is not set. Skipping integration test setup.")
			false
		} else {
			true
		}
	}

	doLast {
		// This logic is deferred to the execution phase, which is safer.
		val tomcatConfOriginal = file("$tomcatHome/conf_original")
		if (!tomcatConfOriginal.exists()) {
			logger.lifecycle("Backing up Tomcat configuration to $tomcatConfOriginal")
			project.copy {
				from("$tomcatHome/conf")
				into(tomcatConfOriginal)
			}
		} else {
			logger.info("Tomcat configuration backup already exists at $tomcatConfOriginal")
		}
	}
}

tasks.register<Test>("integrationTests") {
	group = "Test"
	description = "Run the integration tests, ones that require a tomcat installation."
	forkEvery = 1
	maxParallelForks = 1 // Set to > 1 for parallel execution if tests are isolated
	dependsOn("integrationTestSetup")
	useJUnitPlatform {
		includeTags("integration")
		excludeTags("slow", "flaky")
	}
	finalizedBy("shutdownAllTomcats")
}

tasks.register<Test>("epicsTests") {
	group = "Test"
	description = "Run the epics integration tests with parallel iocs."
	useJUnitPlatform {
		includeTags("localEpics")
		excludeTags("slow", "flaky", "integration")
	}
}

tasks.register<Test>("singleForkTests") {
	group = "Test"
	description = "Run the single fork tests. Ones that require a fork every test."
	forkEvery = 1
	useJUnitPlatform {
		includeTags("singleFork")
		excludeTags("slow", "flaky", "integration", "localEpics")
	}
}

// Configure the default 'test' task
tasks.named<Test>("test") {
	group = "Test"
	useJUnitPlatform {
		excludeTags("integration", "localEpics", "flaky", "singleFork", "slow")
	}
}

// A lifecycle task to run all test suites.
tasks.register("allTests") {
	group = "Verification"
	description = "Run all the tests."
	dependsOn("unitTests", "singleForkTests", "integrationTests", "flakyTests", "epicsTests")
}

tasks.register<Test>("automationTests") {
	group = "Verification"
	description = "Run all the tests in an automated environment"
	forkEvery = 1
	maxParallelForks = 1
	dependsOn("integrationTestSetup")
	useJUnitPlatform {
		// No include/exclude means run all tests
	}
	finalizedBy("shutdownAllTomcats")
}

tasks.register<JavaExec>("testRun") {
	group = "Test"
	description = "Runs the application same as for integration tests."
	dependsOn("integrationTestSetup")

	environment("ARCHAPPL_SHORT_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/sts")
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/mts")
	environment("ARCHAPPL_LONG_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/lts")

	mainClass.set("org.epics.archiverappliance.TestRun")
	classpath = sourceSets.test.get().runtimeClasspath
	args("-c2")
}

// =================================================================
// Code Quality and Formatting
// =================================================================

if (gitWorks) {
	configure<SpotlessExtension> {
		// Optional: limit format enforcement to just the files changed by this feature branch
		ratchetFrom("origin/master")

		format("misc") {
			// Define the files to apply `misc` to
			target("*.gradle.kts", "*.md", ".gitignore")

			// Define the steps to apply to those files
			trimTrailingWhitespace()
			indentWithTabs()
			endWithNewline()
		}
		format("styling") {
			target(
				"docs/docs/source/**/*.html",
				"docs/docs/source/**/*.css",
				"docs/docs/source/**/*.md",
				"docs/**/docs.js",
				"src/main/**/*.html",
				"src/main/**/*.js",
				"src/main/**/*.css"
			)
			prettier()
		}
		java {
			// Exclude the generated java from the protobuf
			targetExclude(fileTree(srcDir) { include("**/EPICSEvent.java") })
			removeUnusedImports()
			// apply a specific flavor of google-java-format
			palantirJavaFormat()
			importOrder("", "java|javax|jakarta", "#")
			// fix formatting of type annotations
			formatAnnotations()
		}
	}
}
// Set the default task to run when you execute 'gradle' with no arguments
defaultTasks("buildRelease")
