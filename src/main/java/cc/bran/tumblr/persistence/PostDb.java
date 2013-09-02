package cc.bran.tumblr.persistence;

import cc.bran.tumblr.types.Post;

/**
 * Represents a database with the ability to persist and retrieve {@link Post}s.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public interface PostDb {

  Post get(long id) throws Exception;

  void put(Post post) throws Exception;
}
