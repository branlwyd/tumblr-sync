package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;

/**
 * Represents a Tumblr text post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class TextPost extends Post {

  private final String body;

  private final String title;

  public TextPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String title, String body) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(title);
    Preconditions.checkNotNull(body);

    this.title = title;
    this.body = body;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof TextPost)) {
      return false;
    }
    TextPost otherPost = (TextPost) other;
    return Objects.equals(this.title, otherPost.title) && Objects.equals(this.body, otherPost.body);
  }

  public String getBody() {
    return body;
  }

  public String getTitle() {
    return title;
  }

  public PostType getType() {
    return PostType.TEXT;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), title, body);
  }
}
