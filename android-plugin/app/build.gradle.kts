import org.apache.tools.ant.taskdefs.condition.Os

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.trueimaptunnel.plugin"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.trueimaptunnel.plugin"
        minSdk = 23
        targetSdk = 35
        versionCode = 2
        versionName = "0.1.1"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
    kotlinOptions.jvmTarget = "1.8"

    sourceSets.getByName("main").jniLibs.srcDirs(files("src/main/jniLibs"))
    packagingOptions.jniLibs.useLegacyPackaging = true
}

tasks.register<Exec>("buildGoArm64") {
    val repoRoot = rootProject.projectDir.parentFile
    val outDir = project.layout.projectDirectory.dir("src/main/jniLibs/arm64-v8a").asFile
    val outFile = outDir.resolve("libtrueimaptunnel.so")
    val versionName = android.defaultConfig.versionName ?: "dev"

    doFirst { outDir.mkdirs() }

    if (Os.isFamily(Os.FAMILY_WINDOWS)) {
        commandLine(
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            repoRoot.resolve("scripts/build-android-plugin-binary.ps1").absolutePath,
            "-Out",
            outFile.absolutePath,
            "-VersionName",
            versionName,
        )
    } else {
        commandLine(
            "bash",
            repoRoot.resolve("scripts/build-android-plugin-binary.sh").absolutePath,
            outFile.absolutePath,
            versionName,
        )
    }
}

tasks.whenTaskAdded {
    if (name == "mergeDebugJniLibFolders" || name == "mergeReleaseJniLibFolders") {
        dependsOn("buildGoArm64")
    }
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("com.github.shadowsocks:plugin:2.0.1")
}
