package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * A Software Heritage persistent identifier (SWHID), see <a href=
 * "https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers">persistent
 * identifier documentation</a>.
 *
 * @author The Software Heritage developers
 */

public class SWHID {
    /** Fixed hash length of the SWHID */
    public static final int HASH_LENGTH = 40;

    /** Full SWHID as a string */
    String swhid;
    /** SWHID node type */
    Node.Type type;

    /**
     * Constructor.
     *
     * @param swhid full SWHID as a string
     */
    public SWHID(String swhid) {
        this.swhid = swhid;

        // SWHID format: 'swh:1:type:hash'
        String[] parts = swhid.split(":");
        if (parts.length != 4 || !parts[0].equals("swh") || !parts[1].equals("1")) {
            throw new IllegalArgumentException("malformed SWHID: " + swhid);
        }
        this.type = Node.Type.fromStr(parts[2]);
        if (!parts[3].matches("[0-9a-f]{" + HASH_LENGTH + "}")) {
            throw new IllegalArgumentException("malformed SWHID: " + swhid);
        }
    }

    /**
     * Creates a SWHID from a compact binary representation.
     * <p>
     * The binary format is specified in the Python module swh.graph.swhid:str_to_bytes .
     */
    public static SWHID fromBytes(byte[] input) {
        byte[] digest = new byte[20];
        System.arraycopy(input, 2, digest, 0, digest.length);

        String swhidStr = String.format("swh:%d:%s:%s", input[0], Node.Type.fromInt(input[1]).toString().toLowerCase(),
                Hex.encodeHexString(digest));
        return new SWHID(swhidStr);
    }

    @Override
    public boolean equals(Object otherObj) {
        if (otherObj == this)
            return true;
        if (!(otherObj instanceof SWHID))
            return false;

        SWHID other = (SWHID) otherObj;
        return swhid.equals(other.getSWHID());
    }

    @Override
    public int hashCode() {
        return swhid.hashCode();
    }

    @Override
    public String toString() {
        return swhid;
    }

    /**
     * Converts SWHID to a compact binary representation.
     * <p>
     * The binary format is specified in the Python module swh.graph.swhid:str_to_bytes .
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[22];
        byte[] digest;

        bytes[0] = (byte) 1; // namespace version
        bytes[1] = (byte) Node.Type.toInt(this.type); // SWHID type
        try {
            digest = Hex.decodeHex(this.swhid.substring(10)); // SHA1 hash
            System.arraycopy(digest, 0, bytes, 2, digest.length);
        } catch (DecoderException e) {
            throw new IllegalArgumentException("invalid hex sequence in SWHID: " + this.swhid);
        }

        return bytes;
    }

    /**
     * Returns full SWHID as a string.
     *
     * @return full SWHID string
     */
    @JsonValue
    public String getSWHID() {
        return swhid;
    }

    /**
     * Returns SWHID node type.
     *
     * @return SWHID corresponding {@link Node.Type}
     * @see org.softwareheritage.graph.Node.Type
     */
    public Node.Type getType() {
        return type;
    }
}
