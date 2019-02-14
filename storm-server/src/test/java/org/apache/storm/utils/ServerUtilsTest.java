/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.storm.testing.TmpPath;
import org.junit.jupiter.api.Test;

public class ServerUtilsTest {


    @Test
    public void testExtractZipFileDisallowsPathTraversal() throws Exception {
        String TEST_RESOURCES_EVIL_JAR = "src/test/resources/evil-path-traversal.jar";
        String TEST_RESOURCES_EVIL_TAR_GZ = "src/test/resources/evil-path-traversal.tar.gz";
        String TEST_RESOURCES_EVIL_ZIP = "src/test/resources/evil-path-traversal.zip";
        String EVIL_TXT_FILE = "evil.txt";

        try (TmpPath path = new TmpPath()) {
            Path testRoot = Paths.get(path.getPath());
            Path extractionDest = testRoot.resolve("dest");
            Files.createDirectories(extractionDest);

            //Contains an entry named resources/../evil.txt
            ServerUtils.unJar(Paths.get(TEST_RESOURCES_EVIL_JAR).toFile(), extractionDest.toFile());

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            ServerUtils.extractDirFromJar(TEST_RESOURCES_EVIL_JAR, "resources" , extractionDest.toFile());

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            ServerUtils.unZip(Paths.get(TEST_RESOURCES_EVIL_ZIP).toFile(), extractionDest.toFile());

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            ServerUtils.unTar(Paths.get(TEST_RESOURCES_EVIL_TAR_GZ).toFile(), extractionDest.toFile(), true);

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            ServerUtils.unpack(Paths.get(TEST_RESOURCES_EVIL_JAR).toFile(), extractionDest.toFile(), false);

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            ServerUtils.unpack(Paths.get(TEST_RESOURCES_EVIL_ZIP).toFile(), extractionDest.toFile(), false);

            assertThat(Files.exists(testRoot.resolve(EVIL_TXT_FILE)), is(false));

            try {
                ServerUtils.unpack(Paths.get(TEST_RESOURCES_EVIL_TAR_GZ).toFile(), extractionDest.toFile(), false);
                fail("Should have failed to unpack the gz file!");
            } catch (IOException e) {
                //Ignore IOException as it is expected.
            }
            assertThat(Files.exists(testRoot.resolve("evil.txt")), is(false));

        }
    }

}
