package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;

import org.softwareheritage.graph.Node;

/**
 * A Software Heritage PID, see <a
 * href="https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers">persistent
 * identifier documentation</a>.
 *
 * @author Thibault Allançon
 */

public class SwhPID {
  /** Fixed hash length of the PID */
  public static final int HASH_LENGTH = 40;

  /** Full PID as a string */
  String swhPID;
  /** PID node type */
  Node.Type type;
  /** PID hex-encoded SHA1 hash */
  String hash;

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
      throw new IllegalArgumentException(
          "Expected SWH PID format to be 'swh:1:type:hash', got: " + swhPID);
    }

    this.type = Node.Type.fromStr(parts[2]);

    this.hash = parts[3];
    if (!hash.matches("[0-9a-f]{" + HASH_LENGTH + "}")) {
      throw new IllegalArgumentException("Wrong SWH PID hash format in: " + swhPID);
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

  /**
   * Returns PID hex-encoded SHA1 hash.
   *
   * @return PID string hash
   */
  public String getHash() {
    return hash;
  }
}
