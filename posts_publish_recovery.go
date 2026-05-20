package threads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// publishRecoveryWindow is the negative buffer applied to the publishStart
// timestamp when querying recent posts during recovery. It absorbs clock skew
// between the local machine and Meta's servers without widening the search
// enough to risk collisions with prior unrelated publishes.
const publishRecoveryWindow = 5 * time.Second

// errPublishNotRecovered is the sentinel returned by tryRecoverPublishedPost
// when no recovery is possible. Callers should surface the original publish
// error in this case, not the recovery error.
var errPublishNotRecovered = errors.New("publish not recovered")

// publishMatcher reports whether a post returned from /me/threads is the
// post that the current publish attempt produced. Matchers are content-type
// specific (carousel matches by child container IDs, image-reply matches by
// reply_to, etc.); see makeXxxMatcher helpers below.
type publishMatcher func(*Post) bool

// shouldAttemptPublishRecovery reports whether err matches the documented
// Meta pattern where /threads_publish returns an error response despite the
// container actually being published. Currently this is code 10
// (GraphMethodException — "Application does not have permission for this
// action"), which Meta returns from /threads_publish for some app/permission
// configurations after-the-fact, with the container moving to PUBLISHED
// regardless. See the Threads API troubleshooting docs (section "Publishing
// Does Not Return a Media ID") for the canonical recovery strategy.
func shouldAttemptPublishRecovery(err error) bool {
	base := extractBaseError(err)
	if base == nil {
		return false
	}
	return isNonRetryablePermanentErrorCode(base.Code)
}

// recoverFromPublishError is the single entry point Create*Post functions
// call after a failed publishContainer. It short-circuits to errPublishNotRecovered
// when the publish error is not one we know to be ambiguous (i.e. not
// code 10), avoiding an extra Meta round-trip for genuine failures.
func (c *Client) recoverFromPublishError(
	ctx context.Context,
	containerID string,
	publishStart time.Time,
	publishErr error,
	match publishMatcher,
) (*Post, error) {
	if !shouldAttemptPublishRecovery(publishErr) {
		return nil, errPublishNotRecovered
	}
	return c.tryRecoverPublishedPost(ctx, containerID, publishStart, match)
}

// tryRecoverPublishedPost implements the Meta-documented recovery flow for
// /threads_publish calls that error out despite succeeding server-side:
//
//  1. Confirm the container's status is PUBLISHED (cheap, single GET). If it
//     isn't, the publish really did fail and there's nothing to recover.
//  2. List recent posts authored by this user since publishStart (minus a
//     small skew buffer) and apply the caller-supplied matcher.
//  3. Return the unique matching post, or errPublishNotRecovered if zero or
//     more than one posts match.
//
// The matcher MUST be content-specific enough to be unique per request even
// under concurrent publishes from the same user. For carousels, that's the
// set of child container IDs (Meta mints fresh IDs per CreateMediaContainer
// call, so two concurrent carousels never share children). For other post
// types, see the corresponding makeXxxMatcher helper.
//
// The function intentionally returns errPublishNotRecovered (not the original
// publish error) when recovery isn't possible — the caller is expected to
// propagate the original error in that case.
func (c *Client) tryRecoverPublishedPost(
	ctx context.Context,
	containerID string,
	publishStart time.Time,
	match publishMatcher,
) (*Post, error) {
	// Gate 1: container must be in PUBLISHED state. This is the canonical
	// signal from Meta that the publish actually succeeded.
	status, err := c.GetContainerStatus(ctx, ContainerID(containerID))
	if err != nil {
		return nil, fmt.Errorf("recovery: failed to fetch container status: %w", err)
	}
	if status.Status != ContainerStatusPublished {
		return nil, errPublishNotRecovered
	}

	// Gate 2: a matching post must exist in the user's recent timeline,
	// posted after we started the publish attempt.
	userID := c.getUserID()
	if userID == "" {
		return nil, NewAuthenticationError(401, "User ID not available", "Cannot determine user ID from token")
	}
	sinceTS := publishStart.Add(-publishRecoveryWindow).Unix()
	posts, err := c.GetUserPostsWithOptions(ctx, UserID(userID), &PostsOptions{
		Limit: 25,
		Since: sinceTS,
	})
	if err != nil {
		return nil, fmt.Errorf("recovery: failed to list recent posts: %w", err)
	}

	// Find a unique match. Fail closed on multiple matches — the matcher
	// is supposed to be unique per request, so >1 match means something
	// is off and we'd rather surface the original error than return the
	// wrong post.
	var found *Post
	for i := range posts.Data {
		if match(&posts.Data[i]) {
			if found != nil {
				return nil, errPublishNotRecovered
			}
			found = &posts.Data[i]
		}
	}
	if found == nil {
		return nil, errPublishNotRecovered
	}
	return found, nil
}

// makeCarouselMatcher returns a matcher that identifies the published
// carousel post by exact equality of its child container ID set. Carousel
// child container IDs are minted per CreateMediaContainer call and are
// globally unique, so this is collision-proof across concurrent publishes.
func makeCarouselMatcher(childContainerIDs []string) publishMatcher {
	expected := make(map[string]struct{}, len(childContainerIDs))
	for _, id := range childContainerIDs {
		expected[id] = struct{}{}
	}
	return func(p *Post) bool {
		if p.MediaType != MediaTypeResponseCarousel {
			return false
		}
		if p.Children == nil || len(p.Children.Data) != len(expected) {
			return false
		}
		for _, child := range p.Children.Data {
			if _, ok := expected[child.ID]; !ok {
				return false
			}
		}
		return true
	}
}

// makeImageMatcher returns a matcher for a single-image post. For replies,
// parent ID alone is not unique — a caller can legitimately publish multiple
// replies to the same parent, and clock skew or propagation delay could put
// a prior reply inside the recovery window. So for replies we additionally
// require exact text equality or a matching quoted-post target; blank-text
// non-quote replies fail closed because they have no unique signal. For
// non-replies, exact text + topic_tag + non-reply state is the disambiguator
// — topic_tag breaks ties between same-text posts across different topics,
// matching makeTextMatcher's behavior. The image URL stored on the post is
// Meta's CDN URL, not ours, so we don't compare it.
func makeImageMatcher(content *ImagePostContent) publishMatcher {
	return func(p *Post) bool {
		if p.MediaType != MediaTypeImage {
			return false
		}
		if !quoteMatches(p, content.QuotedPostID) {
			return false
		}
		if content.ReplyTo == "" {
			return !p.IsReply && p.Text == content.Text && p.TopicTag == content.TopicTag
		}
		if !p.IsReply || repliedToID(p) != content.ReplyTo {
			return false
		}
		if !replyHasUniqueDiscriminator(content.Text, content.QuotedPostID) {
			return false
		}
		return p.Text == content.Text
	}
}

// makeVideoMatcher mirrors makeImageMatcher for video posts. After publish,
// Meta may report video posts as media_type == "VIDEO" or "AUDIO" (for
// audio-only uploads). Accept either. The same reply-uniqueness rules apply:
// blank-text non-quote replies fail closed. Non-replies disambiguate on
// text + topic_tag.
func makeVideoMatcher(content *VideoPostContent) publishMatcher {
	return func(p *Post) bool {
		if p.MediaType != MediaTypeVideo && p.MediaType != MediaTypeAudio {
			return false
		}
		if !quoteMatches(p, content.QuotedPostID) {
			return false
		}
		if content.ReplyTo == "" {
			return !p.IsReply && p.Text == content.Text && p.TopicTag == content.TopicTag
		}
		if !p.IsReply || repliedToID(p) != content.ReplyTo {
			return false
		}
		if !replyHasUniqueDiscriminator(content.Text, content.QuotedPostID) {
			return false
		}
		return p.Text == content.Text
	}
}

// makeTextMatcher returns a matcher for a text-only post. Text posts always
// have non-empty text (validated upstream), so text equality is itself a
// strong discriminator. Quote state must match in both directions, and
// non-replies additionally compare topic_tag to disambiguate same-text posts
// across different topics.
func makeTextMatcher(content *TextPostContent) publishMatcher {
	wantTextType := MediaTypeResponseText
	return func(p *Post) bool {
		// Some text post variants come back as TEXT_POST or omit media_type
		// entirely. Accept both shapes.
		if p.MediaType != "" && p.MediaType != wantTextType && p.MediaType != MediaTypeText {
			return false
		}
		if !quoteMatches(p, content.QuotedPostID) {
			return false
		}
		if p.Text != content.Text {
			return false
		}
		if content.ReplyTo != "" {
			return p.IsReply && repliedToID(p) == content.ReplyTo
		}
		return !p.IsReply && p.TopicTag == content.TopicTag
	}
}

// quoteMatches reports whether a retrieved post's quote state aligns with
// what the caller asked for. wantQuotedID == "" means "must not be a quote
// post"; non-empty means "must be a quote of that specific post". Matching
// across the quote/non-quote boundary would let a regular post masquerade as
// a quote post (or vice-versa) during recovery.
func quoteMatches(p *Post, wantQuotedID string) bool {
	if wantQuotedID == "" {
		return !p.IsQuotePost
	}
	if !p.IsQuotePost || p.QuotedPost == nil {
		return false
	}
	return p.QuotedPost.ID == wantQuotedID
}

// replyHasUniqueDiscriminator reports whether the (text, quotedPostID) pair
// is strong enough to single out one reply among potentially many to the
// same parent. Without one of these signals we'd be guessing.
func replyHasUniqueDiscriminator(text, quotedPostID string) bool {
	return text != "" || quotedPostID != ""
}

// repliedToID returns the parent post ID for a reply, preferring the
// dedicated reply_to string field and falling back to the embedded
// replied_to object. Returns "" if neither is populated.
func repliedToID(p *Post) string {
	if p.ReplyTo != "" {
		return p.ReplyTo
	}
	if p.RepliedTo != nil {
		return p.RepliedTo.ID
	}
	return ""
}
