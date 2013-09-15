package cc.bran.tumblr.types;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.joda.time.Instant;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Represents a Tumblr photo post.
 * 
 * @author Brandon Pitman (brandon.pitman@gmail.com)
 */
public class PhotoPost extends Post {

  public static class Builder extends Post.Builder {

    private String caption;

    private int height;

    private List<Photo> photos;

    private int width;

    @Override
    public PhotoPost build() {
      return new PhotoPost(id, blogName, postUrl, postedInstant, retrievedInstant, tags, photos,
              caption, width, height);
    }

    public void setCaption(String caption) {
      this.caption = caption;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    public void setPhotos(List<Photo> photos) {
      this.photos = ImmutableList.copyOf(photos);
    }

    public void setWidth(int width) {
      this.width = width;
    }
  }

  public static class Photo {

    public static class PhotoSize {

      private final int height;

      private final String url;

      private final int width;

      public PhotoSize(int width, int height, String url) {
        Preconditions.checkNotNull(url);

        this.width = width;
        this.height = height;
        this.url = url;
      }

      @Override
      public boolean equals(Object other) {
        if (!(other instanceof PhotoSize)) {
          return false;
        }
        PhotoSize otherPhotoSize = (PhotoSize) other;
        return Objects.equals(this.width, otherPhotoSize.width)
                && Objects.equals(this.height, otherPhotoSize.height)
                && Objects.equals(this.url, otherPhotoSize.url);
      }

      public int getHeight() {
        return height;
      }

      public String getUrl() {
        return url;
      }

      public int getWidth() {
        return width;
      }

      @Override
      public int hashCode() {
        return Objects.hash(width, height, url);
      }
    }

    private final String caption;

    private final List<PhotoSize> photoSizes;

    public Photo(String caption, List<PhotoSize> photoSizes) {
      Preconditions.checkNotNull(caption);
      Preconditions.checkNotNull(photoSizes);

      this.caption = caption;
      this.photoSizes = ImmutableList.copyOf(photoSizes);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Photo)) {
        return false;
      }
      Photo otherPhoto = (Photo) other;
      return Objects.equals(this.caption, otherPhoto.caption)
              && Objects.equals(this.photoSizes, otherPhoto.photoSizes);
    }

    public String getCaption() {
      return caption;
    }

    public List<PhotoSize> getPhotoSizes() {
      return photoSizes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(caption, photoSizes);
    }
  }

  private final String caption;

  private final int height;

  private final List<Photo> photos;

  private final int width;

  public PhotoPost(long id, String blogName, String postUrl, Instant postedInstant,
          Instant retrievedInstant, Collection<String> tags, List<Photo> photos, String caption,
          int width, int height) {
    super(id, blogName, postUrl, postedInstant, retrievedInstant, tags);
    Preconditions.checkNotNull(photos);
    Preconditions.checkNotNull(caption);

    this.photos = ImmutableList.copyOf(photos);
    this.caption = caption;
    this.width = width;
    this.height = height;
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }
    if (!(other instanceof PhotoPost)) {
      return false;
    }
    PhotoPost otherPost = (PhotoPost) other;
    return Objects.equals(this.photos, otherPost.photos)
            && Objects.equals(this.caption, otherPost.photos)
            && Objects.equals(this.width, otherPost.width)
            && Objects.equals(this.height, otherPost.height);
  }

  public String getCaption() {
    return caption;
  }

  public int getHeight() {
    return height;
  }

  public List<Photo> getPhotos() {
    return photos;
  }

  @Override
  public PostType getType() {
    return PostType.PHOTO;
  }

  public int getWidth() {
    return width;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), photos, caption, width, height);
  }
}
