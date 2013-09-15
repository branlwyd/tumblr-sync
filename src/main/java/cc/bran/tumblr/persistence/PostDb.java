package cc.bran.tumblr.persistence;

import cc.bran.tumblr.types.Post;

/**
 * Represents a database with the ability to persist and retrieve {@link Post}s.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public interface PostDb {

  /**
   * Delete a post from the database. Deleting a nonexistent post is a no-op.
   * 
   * @param id
   *          the ID of the post to delete
   * @throws Exception
   *           if a database error occurs
   */
  void delete(long id) throws Exception;

  /**
   * Gets a post from the database. Returns null if there is no such post.
   * 
   * @param id
   *          the ID of the post to retrieve
   * @return the post, or null
   * @throws Exception
   *           if a database error occurs
   */
  Post get(long id) throws Exception;

  /**
   * Puts a post into the database. If there is already a post with the same ID, it is overwritten.
   * 
   * @param post
   *          the post to put into the database
   * @throws Exception
   *           if a database error occurs
   */
  void put(Post post) throws Exception;
}
