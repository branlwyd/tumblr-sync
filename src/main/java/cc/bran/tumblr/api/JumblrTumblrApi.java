package cc.bran.tumblr.api;

import java.util.Iterator;

import org.joda.time.Instant;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.common.base.Preconditions;
import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.types.TextPost;

import cc.bran.tumblr.types.Post;

/**
 * A {@link TumblrApi} implementation backed by the Jumblr API.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class JumblrTumblrApi implements TumblrApi {

  private class AllPostsIterator implements Iterator<Post> {

    private final String blogName;
    
    public AllPostsIterator(String blogName) {
      this.blogName = blogName;
    }
    
    @Override
    public boolean hasNext() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Post next() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }
  }
  
  private static final long MILLIS_PER_SECOND = 1000;
  
  private final JumblrClient client;
  
  public JumblrTumblrApi(JumblrClient client) {
    this.client = client;
  }
  
  @Override
  public Post getPost(String blogName, long id) {
    return fromJumblrPost(client.blogPost(blogName, id));
  }

  @Override
  public Iterable<Post> getAllPosts(final String blogName) {
    return new Iterable<Post>() {

      @Override
      public Iterator<Post> iterator() {
        return new AllPostsIterator(blogName);
      }
    };
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post) {
    return fromJumblrPost(post, Instant.now());
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post, Instant retrievedInstant) {
    Preconditions.checkArgument(post instanceof TextPost);
    TextPost textPost = (TextPost) post;
    Instant postedInstant = new Instant(MILLIS_PER_SECOND * textPost.getTimestamp());

    return new Post(textPost.getId(), textPost.getBlogName(), textPost.getPostUrl(), postedInstant,
            retrievedInstant, textPost.getTags(), textPost.getTitle(), textPost.getBody());
  }
}
