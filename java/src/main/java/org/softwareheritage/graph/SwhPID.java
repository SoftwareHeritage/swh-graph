package org.softwareheritage.graph;

import java.lang.System;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;

import org.softwareheritage.graph.Node;

/**
 * A Software Heritage PID, see <a
 * href="https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers">persistent
 * identifier documentation</a>.
 *
 * @author The Software Heritage developers
 */

public class SwhPID {
    /** Fixed hash length of the PID */
    public static final int HASH_LENGTH = 40;

    /** Full PID as a string */
    String swhPID;
    /** PID node type */
    Node.Type type;

    /**
     * Constructor.
     *
     * @param swhPID full PID as a string
     */
    public SwhPID(String swhPID) {
        this.swhPID = swhPID;

        // PID format: 'swh:1:type:hash'
        String[] parts = swhPID.split(":");
        if (parts.length != 4 || !parts[0].equals("swh") || !parts[1].equals("1")) {
            throw new IllegalArgumentException("malformed SWH PID: " + swhPID);
        }
        this.type = Node.Type.fromStr(parts[2]);
        if (!parts[3].matches("[0-9a-f]{" + HASH_LENGTH + "}")) {
            throw new IllegalArgumentException("malformed SWH PID: " + swhPID);
        }
    }

    @Override
    public boolean equals(Object otherObj) {
        if (otherObj == this)
            return true;
        if (!(otherObj instanceof SwhPID))
            return false;

        SwhPID other = (SwhPID) otherObj;
        return swhPID.equals(other.getSwhPID());
    }

    @Override
    public int hashCode() {
        return swhPID.hashCode();
    }

    @Override
    public String toString() {
        return swhPID;
    }

    /** Converts PID to a compact binary representation.
     *
     * The binary format is specified in the Python module
     * swh.graph.pid:str_to_bytes .
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[22];
        byte[] digest;

        bytes[0] = (byte) 1;  // namespace version
        bytes[1] = (byte) Node.Type.toInt(this.type);  // PID type
        try {
            digest = Hex.decodeHex(this.swhPID.substring(10));  // SHA1 hash
            System.arraycopy(digest, 0, bytes, 2, digest.length);
        } catch (DecoderException e) {
            throw new IllegalArgumentException("invalid hex sequence in PID: " + this.swhPID);
        }

        return bytes;
    }

    /**
     * Returns full PID as a string.
     *
     * @return full PID string
     */
    @JsonValue
    public String getSwhPID() {
        return swhPID;
    }

    /**
     * Returns PID node type.
     *
     * @return PID corresponding {@link Node.Type}
     * @see org.softwareheritage.graph.Node.Type
     */
    public Node.Type getType() {
        return type;
    }
}
