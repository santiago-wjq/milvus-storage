package io.milvus.storage;

import java.io.*;
import java.nio.file.*;

/**
 * Native library loader that extracts and loads native libraries from JAR resources.
 * Supports multiple platforms and automatic cleanup.
 */
public class NativeLibraryLoader {
    private static boolean loaded = false;
    private static final String JNI_LIBRARY_NAME = "milvus-storage-jni";

    /**
     * Load the native library. This method is thread-safe and will only load once.
     */
    public static synchronized void loadLibrary() {
        if (loaded) {
            return;
        }

        try {
            // Try loading from resources first (for fat jar)
            loadFromResources();
            loaded = true;
            System.out.println("Native libraries loaded from resources");
            return;
        } catch (Exception e) {
            System.out.println("Could not load from resources: " + e.getMessage());
        }

        try {
            // Fall back to system library path
            System.loadLibrary(JNI_LIBRARY_NAME);
            loaded = true;
            System.out.println("Native library loaded from system path");
            return;
        } catch (UnsatisfiedLinkError e) {
            throw new RuntimeException(
                "Failed to load native library '" + JNI_LIBRARY_NAME + "'. " +
                "Please ensure the library is in java.library.path or packaged in the JAR.", e);
        }
    }

    /**
     * Load libraries from JAR resources
     */
    private static void loadFromResources() throws IOException {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch").toLowerCase();
        String libExtension;
        String platform;

        if (os.contains("windows")) {
            libExtension = "dll";
            platform = "windows-" + (arch.contains("64") ? "x86_64" : "x86");
        } else if (os.contains("mac")) {
            libExtension = "dylib";
            platform = "darwin-" + (arch.contains("aarch64") || arch.contains("arm64") ? "aarch64" : "x86_64");
        } else {
            libExtension = "so";
            platform = "linux-" + (arch.contains("aarch64") || arch.contains("arm64") ? "aarch64" : "x86_64");
        }

        // Create temp directory for extracted libraries
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "milvus-storage-native");
        tempDir.mkdirs();

        // Extract all libraries from the platform directory
        // This ensures dependencies are available when loading the JNI library
        extractAllLibraries("/" + platform + "/", tempDir, libExtension);

        // Load the JNI library (dependencies will be found via RPATH)
        String jniLibName = "lib" + JNI_LIBRARY_NAME + "." + libExtension;
        File jniLib = new File(tempDir, jniLibName);
        if (!jniLib.exists()) {
            throw new IOException("Could not find native library: " + jniLib.getAbsolutePath());
        }
        System.load(jniLib.getAbsolutePath());
    }

    /**
     * Extract all libraries from a resource directory to a temp directory
     */
    private static void extractAllLibraries(String resourceDir, File destDir, String libExtension) throws IOException {
        // List of known libraries to extract (order doesn't matter with RPATH)
        String[] knownLibs = {
            "libmilvus-storage-jni", "libmilvus-storage",
            "libarrow", "libarrow_acero", "libarrow_dataset", "libparquet",
            "libprotobuf", "libprotoc", "libcurl", "libssl", "libcrypto",
            "libz", "liblzma", "libzstd", "libglog", "libgflags_nothreads",
            "libfolly", "libfollybenchmark", "libfolly_exception_counter",
            "libfolly_exception_tracer", "libfolly_exception_tracer_base", "libfolly_test_util",
            "libavrocpp", "libboost_context", "libboost_filesystem", "libboost_program_options",
            "libboost_regex", "libboost_system", "libboost_thread",
            "libaws-cpp-sdk-core", "libaws-cpp-sdk-s3", "libaws-cpp-sdk-identity-management"
        };

        for (String libBase : knownLibs) {
            String libName = libBase + "." + libExtension;
            String resourcePath = resourceDir + libName;
            try {
                extractLibraryFromResource(resourcePath, libName, destDir);
            } catch (IOException e) {
                // Library might not exist for this platform, continue
            }
        }
    }

    /**
     * Extract library from JAR resources to a temporary file
     */
    private static File extractLibraryFromResource(String resourcePath, String libName, File destDir) throws IOException {
        InputStream resourceStream = NativeLibraryLoader.class.getResourceAsStream(resourcePath);

        if (resourceStream == null) {
            throw new IOException("Resource not found: " + resourcePath);
        }

        try {
            File tempFile = new File(destDir, libName);

            // Copy resource to temp file
            Files.copy(resourceStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempFile.deleteOnExit();

            return tempFile;
        } finally {
            resourceStream.close();
        }
    }

    /**
     * Check if the native library is loaded.
     */
    public static boolean isLoaded() {
        return loaded;
    }
}