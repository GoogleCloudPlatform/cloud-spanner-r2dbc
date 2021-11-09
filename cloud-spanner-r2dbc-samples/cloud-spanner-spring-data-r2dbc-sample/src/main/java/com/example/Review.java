package com.example;

import com.google.common.base.Objects;

public class Review {
    String reviewerId;
    String reviewerContent;

    public String getReviewerId() {
        return reviewerId;
    }

    public void setReviewerId(String reviewerId) {
        this.reviewerId = reviewerId;
    }

    public String getReviewerContent() {
        return reviewerContent;
    }

    public void setReviewerContent(String reviewerContent) {
        this.reviewerContent = reviewerContent;
    }

    public Review() {
    }

    public Review(String reviewerId, String reviewerContent) {
        this.reviewerId = reviewerId;
        this.reviewerContent = reviewerContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Review review = (Review) o;
        return Objects.equal(reviewerId, review.reviewerId) && Objects.equal(reviewerContent, review.reviewerContent);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(reviewerId, reviewerContent);
    }
}
