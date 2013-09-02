package cc.bran.tumblr.api;

import cc.bran.tumblr.types.Post;

/**
 * Provides access to tumblr through its API.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public interface TumblrApi {
  
  Post getPost(String blogName, long id);
  
  Iterable<Post> getAllPosts(String blogName);
}
