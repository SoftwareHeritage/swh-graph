/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

/**
 * Node type restriction, useful to implement filtering of returned nodes during traversal.
 *
 * @author The Software Heritage developers
 */

public class AllowedNodes {
    public boolean[] restrictedTo;

    /**
     * Constructor.
     *
     * @param nodesFmt a formatted string describing allowed nodes
     */
    public AllowedNodes(String nodesFmt) {
        int nbNodeTypes = Node.Type.values().length;
        this.restrictedTo = new boolean[nbNodeTypes];
        // Special values (null, empty, "*")
        if (nodesFmt == null || nodesFmt.isEmpty()) {
            return;
        }
        if (nodesFmt.equals("*")) {
            // Allows for quick bypass (with simple null check) when no node restriction
            restrictedTo = null;
            return;
        }

        // Format: "nodeType1,nodeType2,[...]"
        String[] nodeTypesStr = nodesFmt.split(",");
        for (String nodeTypeStr : nodeTypesStr) {
            for (Node.Type nodeType : Node.Type.parse(nodeTypeStr)) {
                this.restrictedTo[Node.Type.toInt(nodeType)] = true;
            }
        }
    }

    /**
     * Checks if a given node type is allowed.
     *
     * @param nodeType node type to check
     * @return true if allowed and false otherwise
     */
    public boolean isAllowed(Node.Type nodeType) {
        if (restrictedTo == null)
            return true;
        return restrictedTo[Node.Type.toInt(nodeType)];
    }
}
