/*
 * Copyright (c) 2019-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import java.util.ArrayList;

/**
 * Edge restriction based on node types, used when visiting the graph.
 * <p>
 * <a href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">Software Heritage
 * graph</a> contains multiple node types (contents, directories, revisions, ...) and restricting
 * the traversal to specific node types is necessary for many querying operations:
 * <a href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html">use cases</a>.
 *
 * @author The Software Heritage developers
 */

public class AllowedEdges {
    /**
     * 2D boolean matrix storing access rights for all combination of src/dst node types (first
     * dimension is source, second dimension is destination), when edge restriction is not enforced this
     * array is set to null for early bypass.
     */
    public boolean[][] restrictedTo;

    /**
     * Constructor.
     *
     * @param edgesFmt a formatted string describing <a href=
     *            "https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">allowed
     *            edges</a>
     */
    public AllowedEdges(String edgesFmt) {
        int nbNodeTypes = SwhType.values().length;
        this.restrictedTo = new boolean[nbNodeTypes][nbNodeTypes];
        // Special values (null, empty, "*")
        if (edgesFmt == null || edgesFmt.isEmpty()) {
            return;
        }
        if (edgesFmt.equals("*")) {
            // Allows for quick bypass (with simple null check) when no edge restriction
            restrictedTo = null;
            return;
        }

        // Format: "src1:dst1,src2:dst2,[...]"
        String[] edgeTypes = edgesFmt.split(",");
        for (String edgeType : edgeTypes) {
            String[] nodeTypes = edgeType.split(":");
            if (nodeTypes.length != 2) {
                throw new IllegalArgumentException("Cannot parse edge type: " + edgeType);
            }

            ArrayList<SwhType> srcTypes = SwhType.parse(nodeTypes[0]);
            ArrayList<SwhType> dstTypes = SwhType.parse(nodeTypes[1]);
            for (SwhType srcType : srcTypes) {
                for (SwhType dstType : dstTypes) {
                    restrictedTo[srcType.ordinal()][dstType.ordinal()] = true;
                }
            }
        }
    }

    /**
     * Checks if a given edge can be followed during graph traversal.
     *
     * @param srcType edge source type
     * @param dstType edge destination type
     * @return true if allowed and false otherwise
     */
    public boolean isAllowed(SwhType srcType, SwhType dstType) {
        if (restrictedTo == null)
            return true;
        return restrictedTo[srcType.ordinal()][dstType.ordinal()];
    }

    /**
     * Return a new AllowedEdges instance with reversed edge restrictions. e.g. "src1:dst1,src2:dst2"
     * becomes "dst1:src1,dst2:src2"
     *
     * @return a new AllowedEdges instance with reversed edge restrictions
     */
    public AllowedEdges reverse() {
        AllowedEdges reversed = new AllowedEdges(null);
        reversed.restrictedTo = new boolean[restrictedTo.length][restrictedTo[0].length];
        for (int i = 0; i < restrictedTo.length; i++) {
            for (int j = 0; j < restrictedTo[0].length; j++) {
                reversed.restrictedTo[i][j] = restrictedTo[j][i];
            }
        }
        return reversed;
    }
}
