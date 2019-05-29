package org.softwareheritage.graph;

public class SwhId {
  String swhId;
  String type;
  String hash;

  public SwhId(String swhId) {
    this.swhId = swhId;
    String[] parts = swhId.split(":");
    if (parts.length != 4) {
      throw new IllegalArgumentException("Incorrect SWH ID format: " + swhId);
    }

    // SWH ID format: 'swh:1:type:hash'
    this.type = parts[2];
    this.hash = parts[3];
  }

  public String getType() {
    return type;
  }

  public String getHash() {
    return hash;
  }

  public String toString() {
    return swhId;
  }
}
