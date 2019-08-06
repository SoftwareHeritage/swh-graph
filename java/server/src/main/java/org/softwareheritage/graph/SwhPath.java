package org.softwareheritage.graph;

import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonValue;

import org.softwareheritage.graph.SwhId;

/**
 * Wrapper class to store a list of {@link SwhId}.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 * @see org.softwareheritage.graph.SwhId
 */

public class SwhPath {
  /** Internal list of {@link SwhId} */
  ArrayList<SwhId> path;

  /**
   * Constructor.
   */
  public SwhPath() {
    this.path = new ArrayList<SwhId>();
  }

  /**
   * Constructor.
   *
   * @param swhIds variable number of string PIDs to initialize this path with
   */
  public SwhPath(String ...swhIds) {
    this();
    for (String swhId : swhIds) {
      add(new SwhId(swhId));
    }
  }

  /**
   * Constructor.
   *
   * @param swhIds variable number of {@link SwhId} to initialize this path with
   * @see org.softwareheritage.graph.SwhId
   */
  public SwhPath(SwhId ...swhIds) {
    this();
    for (SwhId swhId : swhIds) {
      add(swhId);
    }
  }

  /**
   * Returns this path as a list of {@link SwhId}.
   *
   * @return list of {@link SwhId} constituting the path
   * @see org.softwareheritage.graph.SwhId
   */
  @JsonValue
  public ArrayList<SwhId> getPath() {
    return path;
  }

  /**
   * Adds a {@link SwhId} to this path.
   *
   * @param {@link SwhId} to add to this path
   * @see org.softwareheritage.graph.SwhId
   */
  public void add(SwhId swhId) {
    path.add(swhId);
  }

  /**
   * Returns the {@link SwhId} at the specified position in this path.
   *
   * @param index position of the {@link SwhId} to return
   * @return {@link SwhId} at the specified position
   * @see org.softwareheritage.graph.SwhId
   */
  public SwhId get(int index) {
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
    if (otherObj == this) return true;
    if (!(otherObj instanceof SwhPath)) return false;

    SwhPath other = (SwhPath) otherObj;
    if (size() != other.size()) {
      return false;
    }

    for (int i = 0; i < size(); i++) {
      SwhId thisSwhId = get(i);
      SwhId otherSwhId = other.get(i);
      if (!thisSwhId.equals(otherSwhId)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    String str = new String();
    for (SwhId swhId : path) {
      str += swhId + "/";
    }
    return str;
  }
}
