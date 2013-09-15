package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;

/**
 * Represents a Tumblr answer post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class AnswerPost extends Post {

  public static class Builder extends Post.Builder {

    private String answer;

    private String askingName;

    private String askingUrl;

    private String question;

    @Override
    public AnswerPost build() {
      return new AnswerPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags,
              askingName, askingUrl, question, answer);
    }

    public void setAnswer(String answer) {
      this.answer = answer;
    }

    public void setAskingName(String askingName) {
      this.askingName = askingName;
    }

    public void setAskingUrl(String askingUrl) {
      this.askingUrl = askingUrl;
    }

    public void setQuestion(String question) {
      this.question = question;
    }
  }

  private final String answer;

  private final String askingName;

  private final String askingUrl;

  private final String question;

  public AnswerPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, String askingName, String askingUrl,
          String question, String answer) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(askingName);
    Preconditions.checkNotNull(askingUrl);
    Preconditions.checkNotNull(question);
    Preconditions.checkNotNull(answer);

    this.askingName = askingName;
    this.askingUrl = askingUrl;
    this.question = question;
    this.answer = answer;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof AnswerPost)) {
      return false;
    }
    AnswerPost otherPost = (AnswerPost) other;
    return Objects.equals(this.askingName, otherPost.askingName)
            && Objects.equals(this.askingUrl, otherPost.askingUrl)
            && Objects.equals(this.question, otherPost.question)
            && Objects.equals(this.answer, otherPost.answer);
  }

  public String getAnswer() {
    return answer;
  }

  public String getAskingName() {
    return askingName;
  }

  public String getAskingUrl() {
    return askingUrl;
  }

  public String getQuestion() {
    return question;
  }

  @Override
  public PostType getType() {
    return PostType.ANSWER;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), askingName, askingUrl, question, answer);
  }
}
