package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents a Tumblr post that may be persisted.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public abstract class Post {

  public static abstract class Builder {

    protected String blogName;

    protected long id;

    protected Instant postedInstant;

    protected String postUrl;

    protected Instant retrievedInstant;

    protected List<String> tags;

    public abstract Post build();

    public void setBlogName(String blogName) {
      this.blogName = blogName;
    }

    public void setId(long id) {
      this.id = id;
    }

    public void setPostedInstant(Instant postedInstant) {
      this.postedInstant = postedInstant;
    }

    public void setPostUrl(String postUrl) {
      this.postUrl = postUrl;
    }

    public void setRetrievedInstant(Instant retrievedInstant) {
      this.retrievedInstant = retrievedInstant;
    }

    public void setTags(Collection<String> tags) {
      this.tags = ImmutableList.copyOf(tags);
    }
  }

  private final String blogName;

  private final long id;

  private final Instant postedInstant;

  private final String postUrl;

  private final Instant retrievedInstant;

  private final List<String> tags;

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
    this.tags = ImmutableList.copyOf(tags);
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

  public List<String> getTags() {
    return tags;
  }

  public abstract PostType getType();

  @Override
  public int hashCode() {
    return Objects.hash(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
  }
}
