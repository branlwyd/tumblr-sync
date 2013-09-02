package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Represents a Tumblr text post that may be persisted.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class Post {

  private final String blogName;

  private final String body;

  private final long id;

  private final Instant postedInstant;

  private final String postUrl;

  private final Instant retrievedInstant;

  private final Set<String> tags;

  private final String title;

  public Post(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String title, String body) {
    Preconditions.checkNotNull(blogName);
    Preconditions.checkNotNull(postUrl);
    Preconditions.checkNotNull(postedInstant);
    Preconditions.checkNotNull(retrievedInstant);
    Preconditions.checkNotNull(tags);
    Preconditions.checkNotNull(title);
    Preconditions.checkNotNull(body);

    this.id = id;
    this.blogName = blogName;
    this.postUrl = postUrl;
    this.postedInstant = postedInstant;
    this.retrievedInstant = retrievedInstant;
    this.tags = ImmutableSet.copyOf(tags);
    this.title = title;
    this.body = body;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Post)) {
      return false;
    }
    Post otherPost = (Post) other;
    return Objects.equals(this.id, otherPost.id)
            && Objects.equals(this.blogName, otherPost.blogName)
            && Objects.equals(this.postUrl, otherPost.postUrl)
            && Objects.equals(this.postedInstant, otherPost.postedInstant)
            && Objects.equals(this.retrievedInstant, otherPost.retrievedInstant)
            && Objects.equals(this.tags, otherPost.tags)
            && Objects.equals(this.title, otherPost.title)
            && Objects.equals(this.body, otherPost.body);
  }

  public String getBlogName() {
    return blogName;
  }

  public String getBody() {
    return body;
  }

  public long getId() {
    return id;
  }

  public Instant getPostedInstant() {
    return postedInstant;
  }

  public String getPostUrl() {
    return postUrl;
  }

  public Instant getRetrievedInstant() {
    return retrievedInstant;
  }

  public Set<String> getTags() {
    return tags;
  }

  public String getTitle() {
    return title;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, blogName, postUrl, postedInstant, retrievedInstant, tags, title, body);
  }
}
