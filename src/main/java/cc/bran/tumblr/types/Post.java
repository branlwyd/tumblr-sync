package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

/**
 * Represents a Tumblr post that may be persisted.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public abstract class Post {

  private final String blogName;

  private final long id;

  private final Instant postedInstant;

  private final String postUrl;

  private final Instant retrievedInstant;

  private final Set<String> tags;

  public Post(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags) {
    Preconditions.checkNotNull(blogName);
    Preconditions.checkNotNull(postUrl);
    Preconditions.checkNotNull(postedInstant);
    Preconditions.checkNotNull(retrievedInstant);
    Preconditions.checkNotNull(tags);

    this.id = id;
    this.blogName = blogName;
    this.postUrl = postUrl;
    this.postedInstant = postedInstant;
    this.retrievedInstant = retrievedInstant;
    this.tags = ImmutableSet.copyOf(tags);
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
            && Objects.equals(this.tags, otherPost.tags);
  }

  public String getBlogName() {
    return blogName;
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

  public abstract PostType getType();

  @Override
  public int hashCode() {
    return Objects.hash(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
  }
}
