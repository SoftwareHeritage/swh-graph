package org.softwareheritage.graph;

import com.fasterxml.jackson.annotation.JsonValue;

import org.softwareheritage.graph.Node;

/**
 * A Software Heritage PID, see <a
 * href="https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers">persistent
 * identifier documentation</a>.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class SwhId {
  /** Fixed hash length of the PID */
  public static final int HASH_LENGTH = 40;

  /** Full PID as a string */
  String swhId;
  /** PID node type */
  Node.Type type;
  /** PID hex-encoded SHA1 hash */
  String hash;

  /**
   * Constructor.
   *
   * @param swhId full PID as a string
   */
  public SwhId(String swhId) {
    this.swhId = swhId;

    // PID format: 'swh:1:type:hash'
    String[] parts = swhId.split(":");
    if (parts.length != 4 || !parts[0].equals("swh") || !parts[1].equals("1")) {
      throw new IllegalArgumentException("Expected SWH ID format to be 'swh:1:type:hash', got: " + swhId);
    }

    this.type = Node.Type.fromStr(parts[2]);

    this.hash = parts[3];
    if (!hash.matches("[0-9a-f]{" + HASH_LENGTH + "}")) {
      throw new IllegalArgumentException("Wrong SWH ID hash format in: " + swhId);
    }
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) return true;
    if (!(otherObj instanceof SwhId)) return false;

    SwhId other = (SwhId) otherObj;
    return swhId.equals(other.getSwhId());
  }

  @Override
  public int hashCode() {
    return swhId.hashCode();
  }

  @Override
  public String toString() {
    return swhId;
  }

  /**
   * Returns full PID as a string.
   *
   * @return full PID string
   */
  @JsonValue
  public String getSwhId() {
    return swhId;
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
