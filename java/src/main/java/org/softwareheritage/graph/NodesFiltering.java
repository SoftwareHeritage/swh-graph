package org.softwareheritage.graph;

import java.util.ArrayList;

/**
 * <h3>NodesFiltering</h3>
 * <p>
 * class that manages the filtering of nodes that have been returned after a visit of the graph.
 * parameterized by a string that represents either no filtering (*) or a set of node types.
 * </p>
 *
 * <ul>
 *
 * <li>graph/visit/nodes/swh:1:rel:0000000000000000000000000000000000000010 return_types==rev will
 * only return 'rev' nodes.</li>
 *
 * <li>graph/visit/nodes/swh:1:rel:0000000000000000000000000000000000000010
 * return_types==rev,snp,cnt will only return 'rev' 'snp' 'cnt' nodes.</li>
 *
 * <li>graph/visit/nodes/swh:1:rel:0000000000000000000000000000000000000010 return_types==* will
 * return all the nodes.</li>
 * </ul>
 *
 * How to use NodesFiltering :
 *
 * <pre>
 * {@code
 *  Long id1 = .... // graph.getNodeType(id1) == CNT
 *  Long id2 = .... // graph.getNodeType(id2) == SNP
 *  Long id3 = .... // graph.getNodeType(id3) == ORI
 *  ArrayList<Long> nodeIds = nez ArrayList<Long>();
 *  nodeIds.add(id1); nodeIds.add(id2); nodeIds.add(id3);
 *
 *  NodeFiltering nds = new NodesFiltering("snp,ori"); // we allow only snp node types to be shown
 *  System.out.println(nds.filterByNodeTypes(nodeIds,graph)); // will print id2, id3
 *
 *  nds = NodesFiltering("*");
 *  System.out.println(nds.filterByNodeTypes(nodeIds,graph)); // will print id1, id2 id3
 *
 * }
 * </pre>
 */

public class NodesFiltering {

    boolean restricted;
    ArrayList<Node.Type> allowedNodesTypes;

    /**
     * Default constructor, in order to handle the * case (all types of nodes are allowed to be
     * returned). allowedNodesTypes will contains [SNP,CNT....] all types of nodes.
     *
     */
    public NodesFiltering() {
        restricted = false;
        allowedNodesTypes = Node.Type.parse("*");
    }

    /**
     * Constructor
     *
     * @param strTypes a formatted string describing the types of nodes we want to allow to be shown.
     *
     *            NodesFilterind("cnt,snp") will set allowedNodesTypes to [CNT,SNP]
     *
     */
    public NodesFiltering(String strTypes) {
        restricted = true;
        allowedNodesTypes = new ArrayList<Node.Type>();
        String[] types = strTypes.split(",");
        for (String type : types) {
            allowedNodesTypes.add(Node.Type.fromStr(type));
        }
    }

    /**
     * Check if the type given in parameter is in the list of allowed types.
     *
     * @param typ the type of the node.
     */
    public boolean typeIsAllowed(Node.Type typ) {
        return this.allowedNodesTypes.contains(typ);
    }

    /**
     * <p>
     * the function that filters the nodes returned, we browse the list of nodes found after a visit and
     * we create a new list with only the nodes that have a type that is contained in the list of
     * allowed types (allowedNodesTypes)
     * </p>
     *
     * @param nodeIds the nodes founded during the visit
     * @param g the graph in order to find the types of nodes from their id in nodeIds
     * @return a new list with the id of node which have a type in allowedTypes
     *
     *
     */
    public ArrayList<Long> filterByNodeTypes(ArrayList<Long> nodeIds, Graph g) {
        ArrayList<Long> filteredNodes = new ArrayList<Long>();
        for (Long node : nodeIds) {
            if (this.typeIsAllowed(g.getNodeType(node))) {
                filteredNodes.add(node);
            }
        }
        return filteredNodes;
    }
}
