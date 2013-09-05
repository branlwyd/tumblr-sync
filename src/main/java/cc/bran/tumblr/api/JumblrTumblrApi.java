package cc.bran.tumblr.api;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.Instant;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import cc.bran.tumblr.types.Post;
import cc.bran.tumblr.types.TextPost;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.tumblr.jumblr.JumblrClient;

/**
 * A {@link TumblrApi} implementation backed by the Jumblr API.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class JumblrTumblrApi implements TumblrApi {

  private class AllPostsIterator implements Iterator<Post> {

    private final String blogName;

    private int offset;

    private List<Post> queuedPosts;

    public AllPostsIterator(String blogName) {
      this.blogName = blogName;
      this.queuedPosts = new LinkedList<Post>();
      this.offset = 0;

      requestPosts();
    }

    @Override
    public boolean hasNext() {
      return !queuedPosts.isEmpty();
    }

    @Override
    public Post next() {
      Post post = queuedPosts.get(0);
      queuedPosts.remove(0);

      if (queuedPosts.isEmpty()) {
        requestPosts();
      }

      return post;
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }

    private void requestPosts() {
      Instant retrievedInstant = Instant.now();
      List<com.tumblr.jumblr.types.Post> posts = client.blogPosts(blogName,
              ImmutableMap.of("type", "text", "offset", offset));
      offset += posts.size();

      for (com.tumblr.jumblr.types.Post post : posts) {
        queuedPosts.add(fromJumblrPost(post, retrievedInstant));
      }
    }
  }

  private static final long MILLIS_PER_SECOND = 1000;

  private final JumblrClient client;

  public JumblrTumblrApi(JumblrClient client) {
    this.client = client;
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

  @Override
  public Post getPost(String blogName, long id) {
    return fromJumblrPost(client.blogPost(blogName, id));
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post) {
    return fromJumblrPost(post, Instant.now());
  }

  private static Post fromJumblrPost(com.tumblr.jumblr.types.Post post, Instant retrievedInstant) {
    Preconditions.checkArgument(post instanceof com.tumblr.jumblr.types.TextPost);
    com.tumblr.jumblr.types.TextPost textPost = (com.tumblr.jumblr.types.TextPost) post;
    Instant postedInstant = new Instant(MILLIS_PER_SECOND * textPost.getTimestamp());

    return new TextPost(textPost.getId(), textPost.getBlogName(), textPost.getPostUrl(),
            postedInstant, retrievedInstant, textPost.getTags(), Strings.nullToEmpty(textPost
                    .getTitle()), textPost.getBody());
  }
}
