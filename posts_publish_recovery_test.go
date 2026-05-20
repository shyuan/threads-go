package threads

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// containerStatusHandler returns a GET handler for /{containerID} that
// reports FINISHED for the first call (so waitForContainerReady can proceed)
// and the supplied recovery status for every subsequent call. This mirrors
// the real container lifecycle: a container is FINISHED before publish,
// then PUBLISHED (or still FINISHED, on real failure) after.
func containerStatusHandler(statusAfterPublish string) (http.HandlerFunc, *int32) {
	var calls int32
	h := func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		if n == 1 {
			_, _ = w.Write([]byte(`{"id":"the_container","status":"FINISHED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"the_container","status":"` + statusAfterPublish + `"}`))
	}
	return h, &calls
}

// TestCreateCarouselPost_RecoversAfterCode10 verifies the documented Meta
// quirk where /threads_publish returns HTTP 500 + code 10 even though the
// carousel was actually published. After the failed publish, recovery should:
//   - confirm container status is PUBLISHED,
//   - locate the published post via /me/threads, matching by exact child
//     container ID set,
//   - return that post as if the publish had succeeded.
func TestCreateCarouselPost_RecoversAfterCode10(t *testing.T) {
	var publishAttempts int32
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			atomic.AddInt32(&publishAttempts, 1)
			// Mimic Meta: HTTP 500 with code 10 even though the publish
			// will actually have been persisted server-side.
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test-trace"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/child_1"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"child_1","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/child_2"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"child_2","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Recovery's /me/threads lookup. Include the matching post plus
			// an unrelated one to verify the matcher disambiguates.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"other_post","media_type":"CAROUSEL_ALBUM","children":{"data":[{"id":"unrelated_a"},{"id":"unrelated_b"}]}},
                {"id":"recovered_post","media_type":"CAROUSEL_ALBUM","children":{"data":[{"id":"child_1"},{"id":"child_2"}]}}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "carousel",
		Children: []string{"child_1", "child_2"},
	})
	if err != nil {
		t.Fatalf("expected recovery to succeed, got error: %v", err)
	}
	if post == nil || post.ID != "recovered_post" {
		t.Fatalf("expected recovered_post, got %#v", post)
	}
	if got := atomic.LoadInt32(&publishAttempts); got != 1 {
		t.Errorf("expected exactly 1 publish attempt (code 10 non-retryable), got %d", got)
	}
}

// TestCreateCarouselPost_NoRecoveryWhenContainerNotPublished verifies that
// when the container is NOT in PUBLISHED state, we surface the original
// publish error instead of fabricating a recovery. This protects against
// returning the wrong post when Meta really did reject the publish.
func TestCreateCarouselPost_NoRecoveryWhenContainerNotPublished(t *testing.T) {
	// Container stays FINISHED for both the wait-for-ready poll and the
	// recovery status check — the publish really did fail.
	containerStatus, _ := containerStatusHandler("FINISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/child_"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"child","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "carousel",
		Children: []string{"child_1", "child_2"},
	})
	if err == nil {
		t.Fatal("expected the original publish error to surface when container is not PUBLISHED")
	}
	// BaseError.Error() formats as "threads api error 10 (api_error): ...";
	// match that prefix rather than a more fragile substring.
	if !strings.Contains(err.Error(), "threads api error 10") {
		t.Errorf("expected wrapped code 10 error, got: %v", err)
	}
}

// TestCreateCarouselPost_NoRecoveryWhenNoMatch verifies we don't blindly
// pick "the newest post since T0". If no post in the recovery window has
// our child container IDs, recovery fails closed.
func TestCreateCarouselPost_NoRecoveryWhenNoMatch(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/child_"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"child","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Recent posts exist but none have our children — must NOT
			// accidentally pick the newest unrelated one.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"unrelated_a","media_type":"CAROUSEL_ALBUM","children":{"data":[{"id":"x"},{"id":"y"}]}},
                {"id":"unrelated_b","media_type":"IMAGE"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "carousel",
		Children: []string{"child_1", "child_2"},
	})
	if err == nil {
		t.Fatal("expected error when no post matches our child container IDs")
	}
}

// TestCreateCarouselPost_FailClosedOnMultipleMatches: the matcher is supposed
// to be unique per request, so >1 match means we'd be guessing. Confirm we
// fail closed in that case rather than returning the wrong post.
func TestCreateCarouselPost_FailClosedOnMultipleMatches(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/child_"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"child","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Two posts share the same child container IDs — should not
			// happen in practice (Meta mints fresh IDs) but exercise the
			// fail-closed branch defensively.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"match_a","media_type":"CAROUSEL_ALBUM","children":{"data":[{"id":"child_1"},{"id":"child_2"}]}},
                {"id":"match_b","media_type":"CAROUSEL_ALBUM","children":{"data":[{"id":"child_1"},{"id":"child_2"}]}}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "carousel",
		Children: []string{"child_1", "child_2"},
	})
	if err == nil {
		t.Fatal("expected error on ambiguous match (>1)")
	}
}

// TestCreateImagePost_RecoversReplyByParentIDAndText covers the single-image
// reply case where the caller supplies non-empty text. Parent ID alone is
// not unique across replies, so the matcher also requires text equality.
func TestCreateImagePost_RecoversReplyByParentIDAndText(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Two replies to the same parent, only one matches our text.
			// Note: PostExtendedFields requests `replied_to` (object), not
			// `reply_to` (string), so the production API populates p.RepliedTo
			// — mirror that shape here so the test exercises the same code
			// path repliedToID() takes in production.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"earlier_reply","media_type":"IMAGE","is_reply":true,"replied_to":{"id":"parent_post_id"},"text":"earlier comment"},
                {"id":"recovered_reply","media_type":"IMAGE","is_reply":true,"replied_to":{"id":"parent_post_id"},"text":"the comment we just sent"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		ImageURL: "https://example.com/img.jpg",
		ReplyTo:  "parent_post_id",
		Text:     "the comment we just sent",
	})
	if err != nil {
		t.Fatalf("expected recovery for image reply, got: %v", err)
	}
	if post == nil || post.ID != "recovered_reply" {
		t.Fatalf("expected recovered_reply, got %#v", post)
	}
}

// TestCreateImagePost_BlankTextReplyFailsClosed verifies that an image reply
// with no text and no quoted post — which has no unique discriminator beyond
// parent ID — does NOT recover, even if a prior reply to the same parent is
// visible in the recovery window. This protects callers (e.g. chained reply
// flows) from threading subsequent work off the wrong post ID.
func TestCreateImagePost_BlankTextReplyFailsClosed(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// A prior reply to the same parent. Without a discriminator,
			// matching by parent ID alone would WRONGLY return this post.
			// Use `replied_to` (object) to match the production API shape
			// returned for PostExtendedFields.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"prior_reply","media_type":"IMAGE","is_reply":true,"replied_to":{"id":"parent_post_id"},"text":""}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		ImageURL: "https://example.com/img.jpg",
		ReplyTo:  "parent_post_id",
		// Text and QuotedPostID both empty — no unique signal.
	})
	if err == nil {
		t.Fatal("expected fail-closed for blank-text non-quote image reply (would otherwise return prior_reply)")
	}
	if !strings.Contains(err.Error(), "threads api error 10") {
		t.Errorf("expected original code 10 error to surface, got: %v", err)
	}
}

// TestCreateImagePost_QuotePostNotMistakenForRegular verifies the quote-state
// gate: if the caller asked for a non-quote image but a same-text quote post
// shows up in the recovery window, the matcher must NOT pick it.
func TestCreateImagePost_QuotePostNotMistakenForRegular(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Same text, but it's a quote post — we asked for a regular
			// post, so it must NOT match.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"a_quote_post","media_type":"IMAGE","is_reply":false,"text":"hello","is_quote_post":true,"quoted_post":{"id":"some_other_post"}}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "hello",
		ImageURL: "https://example.com/img.jpg",
		// No QuotedPostID → must only match non-quote posts.
	})
	if err == nil {
		t.Fatal("expected fail-closed when only candidate is a quote post and caller asked for non-quote")
	}
}

// TestCreateImagePost_RecoversRootByText covers the non-reply image case
// where we match on exact text equality. A different post with different
// text in the recovery window must NOT be picked.
func TestCreateImagePost_RecoversRootByText(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"different_text_post","media_type":"IMAGE","is_reply":false,"text":"different text"},
                {"id":"matching_post","media_type":"IMAGE","is_reply":false,"text":"hello world"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "hello world",
		ImageURL: "https://example.com/img.jpg",
	})
	if err != nil {
		t.Fatalf("expected recovery via text match, got: %v", err)
	}
	if post == nil || post.ID != "matching_post" {
		t.Fatalf("expected matching_post, got %#v", post)
	}
}

// TestCreateTextPost_RecoversAfterCode10 covers the text-post recovery path
// end-to-end. Specifically, it exercises makeTextMatcher's TopicTag arm: two
// posts in the recovery window share the same text but only one has our
// topic_tag, so the matcher must disambiguate on TopicTag rather than just
// returning the first text-match it encounters.
func TestCreateTextPost_RecoversAfterCode10(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// Two non-reply text posts with identical text — only the
			// matching topic_tag should disambiguate them.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"wrong_topic_post","media_type":"TEXT_POST","is_reply":false,"text":"daily standup","topic_tag":"OtherTopic"},
                {"id":"recovered_post","media_type":"TEXT_POST","is_reply":false,"text":"daily standup","topic_tag":"MorningStandup"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateTextPost(context.Background(), &TextPostContent{
		Text:     "daily standup",
		TopicTag: "MorningStandup",
	})
	if err != nil {
		t.Fatalf("expected text-post recovery to succeed, got: %v", err)
	}
	if post == nil || post.ID != "recovered_post" {
		t.Fatalf("expected recovered_post, got %#v", post)
	}
}

// TestRecovery_NotTriggeredForNonCode10 verifies recovery is gated on the
// code-10 pattern. For other publish failures (e.g. validation error code
// 100), recovery must NOT issue the /me/threads lookup — that lookup is the
// signal that recovery actually fired.
func TestRecovery_NotTriggeredForNonCode10(t *testing.T) {
	var threadsListCalls int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			// Validation error (code 100), not the ambiguous publish pattern.
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"Param creation_id must be a number","type":"THApiException","code":100,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"image_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/image_container"):
			// Container is ready for publish (FINISHED is the expected
			// state at this point in the flow).
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"image_container","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			// If this is hit, recovery erroneously fired for non-code-10.
			atomic.AddInt32(&threadsListCalls, 1)
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "hello",
		ImageURL: "https://example.com/img.jpg",
	})
	if err == nil {
		t.Fatal("expected error from code 100")
	}
	if got := atomic.LoadInt32(&threadsListCalls); got != 0 {
		t.Errorf("recovery should not fire for code 100; /me/threads was queried %d times", got)
	}
}

// TestRecovery_NotRecoveredErrorSurfacesOriginal verifies that when
// recovery is attempted but fails to find a matching post, the caller-facing
// path surfaces the original publish error — not errPublishNotRecovered.
func TestRecovery_NotRecoveredErrorSurfacesOriginal(t *testing.T) {
	containerStatus, _ := containerStatusHandler("PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"Application does not have permission for this action","type":"THApiException","code":10,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"the_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/the_container"):
			containerStatus(w, r)
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[]}`)) // no matches
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "hello",
		ImageURL: "https://example.com/img.jpg",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, errPublishNotRecovered) {
		t.Fatal("expected the original publish error to surface, not errPublishNotRecovered")
	}
	if !strings.Contains(err.Error(), "threads api error 10") {
		t.Errorf("expected original publish error (code 10) in chain, got: %v", err)
	}
}
