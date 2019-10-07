package org.softwareheritage.graph;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonValue;

import org.softwareheritage.graph.SwhPID;

/**
 * Wrapper class to store a list of {@link SwhPID}.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.SwhPID
 */

public class SwhPath {
    /** Internal list of {@link SwhPID} */
    ArrayList<SwhPID> path;

    /**
     * Constructor.
     */
    public SwhPath() {
        this.path = new ArrayList<SwhPID>();
    }

    /**
     * Constructor.
     *
     * @param swhPIDs variable number of string PIDs to initialize this path with
     */
    public SwhPath(String... swhPIDs) {
        this();
        for (String swhPID : swhPIDs) {
            add(new SwhPID(swhPID));
        }
    }

    /**
     * Constructor.
     *
     * @param swhPIDs variable number of {@link SwhPID} to initialize this path with
     * @see org.softwareheritage.graph.SwhPID
     */
    public SwhPath(SwhPID... swhPIDs) {
        this();
        for (SwhPID swhPID : swhPIDs) {
            add(swhPID);
        }
    }

    /**
     * Returns this path as a list of {@link SwhPID}.
     *
     * @return list of {@link SwhPID} constituting the path
     * @see org.softwareheritage.graph.SwhPID
     */
    @JsonValue
    public ArrayList<SwhPID> getPath() {
        return path;
    }

    /**
     * Adds a {@link SwhPID} to this path.
     *
     * @param swhPID {@link SwhPID} to add to this path
     * @see org.softwareheritage.graph.SwhPID
     */
    public void add(SwhPID swhPID) {
        path.add(swhPID);
    }

    /**
     * Returns the {@link SwhPID} at the specified position in this path.
     *
     * @param index position of the {@link SwhPID} to return
     * @return {@link SwhPID} at the specified position
     * @see org.softwareheritage.graph.SwhPID
     */
    public SwhPID get(int index) {
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
            SwhPID thisSwhPID = get(i);
            SwhPID otherSwhPID = other.get(i);
            if (!thisSwhPID.equals(otherSwhPID)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        String str = new String();
        for (SwhPID swhPID : path) {
            str += swhPID + "/";
        }
        return str;
    }
}
