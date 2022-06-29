/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Sort {
    public static Process spawnSort(String sortBufferSize, String sortTmpDir) throws IOException {
        return spawnSort(sortBufferSize, sortTmpDir, null);
    }

    public static Process spawnSort(String sortBufferSize, String sortTmpDir, List<String> options) throws IOException {
        ProcessBuilder sortProcessBuilder = new ProcessBuilder();
        sortProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        ArrayList<String> command = new ArrayList<>(List.of("sort", "-u", "--buffer-size", sortBufferSize));
        if (sortTmpDir != null) {
            command.add("--temporary-directory");
            command.add(sortTmpDir);
        }
        if (options != null) {
            command.addAll(options);
        }
        sortProcessBuilder.command(command);
        Map<String, String> env = sortProcessBuilder.environment();
        env.put("LC_ALL", "C");
        env.put("LC_COLLATE", "C");
        env.put("LANG", "C");

        return sortProcessBuilder.start();
    }
}
