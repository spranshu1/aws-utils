## AWS Utils
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4762c5e21ef54031ad97e8cc6deeec3f)](https://www.codacy.com/manual/pranshushrivastava20/aws-utils?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=spranshu1/aws-utils&amp;utm_campaign=Badge_Grade) ![Maven Build](https://github.com/spranshu1/aws-utils/workflows/Maven%20Build/badge.svg?branch=master)

 A wrapper on top of AWS SDK, contains useful utility and helper classes.

## Target Version

[![Maven Central](https://img.shields.io/maven-central/v/com.github.spranshu1/aws-utils.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.github.spranshu1%22%20AND%20a:%22aws-utils%22)

Getting started:

* [Prerequisites](#markdown-header-prerequisites)
* [Setup](#markdown-header-setup)
* [Release Log](#markdown-header-releaselog)
* [Contact](#markdown-header-authors)

## Prerequisites

Ensure local installation of following softwares/tools:

* JDK - 1.8
    ```markdown
    $ java -version
    java version "1.8.0_121"
    ```
* Apache Maven - if using maven dependency 
    ```https://maven.apache.org/install.html```
* Gradle - if using gradle
    ```https://gradle.org/install/```

## Setup

### Apache Maven

* Add dependency in your `pom.xml`

	```markdown
	
	<dependency>
      <groupId>com.github.spranshu1</groupId>
      <artifactId>aws-utils</artifactId>
      <version>${version}</version>
    </dependency>
	
	```

## Release Log

`1.0.0`

- `IAMHelper` introduced for IAM specific operations
- Minor BugFix in `S3BucketHelper`   
	
`0.0.1`

- First version	

### Support or Contact
```
spranshu1
```
