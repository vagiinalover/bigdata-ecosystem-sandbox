#!/bin/bash

# Ensure output directory exists
mkdir -p build

# Find Spark JARs for classpath
SPARK_JARS=$(find /opt/spark/jars -name "*.jar" | tr '\n' ':')

echo "Compiling Java code..."
javac -classpath "$SPARK_JARS" -d build SizeBasedParquetOutputFormat.java

if [ $? -eq 0 ]; then
    echo "Compilation successful. Packaging JAR..."
    jar cf custom-formats.jar -C build .
    echo "JAR created: custom-formats.jar"
else
    echo "Compilation failed!"
    exit 1
fi
