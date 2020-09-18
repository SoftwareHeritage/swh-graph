package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;

/**
 * Wrapper class to store a list of {@link SWHID}.
 *
 * @author The Software Heritage developers
 * @see SWHID
 */

public class SwhPath {
    /** Internal list of {@link SWHID} */
    ArrayList<SWHID> path;

    /**
     * Constructor.
     */
    public SwhPath() {
        this.path = new ArrayList<>();
    }

    /**
     * Constructor.
     *
     * @param swhids variable number of string SWHIDs to initialize this path with
     */
    public SwhPath(String... swhids) {
        this();
        for (String swhid : swhids) {
            add(new SWHID(swhid));
        }
    }

    /**
     * Constructor.
     *
     * @param swhids variable number of {@link SWHID} to initialize this path with
     * @see SWHID
     */
    public SwhPath(SWHID... swhids) {
        this();
        for (SWHID swhid : swhids) {
            add(swhid);
        }
    }

    /**
     * Returns this path as a list of {@link SWHID}.
     *
     * @return list of {@link SWHID} constituting the path
     * @see SWHID
     */
    @JsonValue
    public ArrayList<SWHID> getPath() {
        return path;
    }

    /**
     * Adds a {@link SWHID} to this path.
     *
     * @param swhid {@link SWHID} to add to this path
     * @see SWHID
     */
    public void add(SWHID swhid) {
        path.add(swhid);
    }

    /**
     * Returns the {@link SWHID} at the specified position in this path.
     *
     * @param index position of the {@link SWHID} to return
     * @return {@link SWHID} at the specified position
     * @see SWHID
     */
    public SWHID get(int index) {
        return path.get(index);
    }

    /**
     * Returns the number of elements in this path.
     *
     * @return number of elements in this path
     */
    public int size() {
        return path.size();
    }

    @Override
    public boolean equals(Object otherObj) {
        if (otherObj == this)
            return true;
        if (!(otherObj instanceof SwhPath))
            return false;

        SwhPath other = (SwhPath) otherObj;
        if (size() != other.size()) {
            return false;
        }

        for (int i = 0; i < size(); i++) {
            SWHID thisSWHID = get(i);
            SWHID otherSWHID = other.get(i);
            if (!thisSWHID.equals(otherSWHID)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        for (SWHID swhid : path) {
            str.append(swhid).append("/");
        }
        return str.toString();
    }
}
