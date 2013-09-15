package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;

/**
 * Represents a Tumblr link post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class LinkPost extends Post {

  public static class Builder extends Post.Builder {

    private String description;

    private String title;

    private String url;

    @Override
    public LinkPost build() {
      return new LinkPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags, title, url,
              description);
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setTitle(String title) {
      this.title = title;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }

  private final String description;

  private final String title;

  private final String url;

  public LinkPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String title, String url,
          String description) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(title);
    Preconditions.checkNotNull(url);
    Preconditions.checkNotNull(description);

    this.title = title;
    this.url = url;
    this.description = description;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof LinkPost)) {
      return false;
    }
    LinkPost otherPost = (LinkPost) other;
    return Objects.equals(this.title, otherPost.title) && Objects.equals(this.url, otherPost.url)
            && Objects.equals(this.description, otherPost.description);
  }

  public String getDescription() {
    return description;
  }

  public String getTitle() {
    return title;
  }

  @Override
  public PostType getType() {
    return PostType.LINK;
  }

  public String getUrl() {
    return url;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), title, url, description);
  }
}
