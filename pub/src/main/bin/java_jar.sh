#!/bin/bash

#실행 예시 ex) ./JsonPrep.sh [파일경로] [로그파일이름]

# Java 실행 파일의 경로
JAVA_PATH="/home/mz1/java_jar/java_config/openjdk-8u342-b07/bin/java"

# 실행할 jar 파일과 인자
#JAR_FILE="JsonPrep.jar" 처럼 jar 이름 기입
JAR_FILE="JsonPrep.jar"
INPUT_FILE="$1"
# 로그파일 이름
LOG_SUFFIX="$2"

# 로그 파일 경로 설정
LOG_FILE="/home/mz1/java_jar/logs/$LOG_SUFFIX.log"

# Java 명령 실행 (stdout과 stderr을 로그 파일로 리다이렉트)
nohup $JAVA_PATH -jar $JAR_FILE "$INPUT_FILE" > "$LOG_FILE" 2>&1 &

exit
