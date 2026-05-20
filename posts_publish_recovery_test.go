package threads

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain shrinks recoveryPollInterval so polling-path tests don't pay
// the production 1s-per-attempt wait. All recovery tests in this file
// rely on this; running them with the default interval would add several
// seconds of wall time per case.
//
// No restore needed: os.Exit terminates the process so any subsequent
// statements would be dead code.
func TestMain(m *testing.M) {
	recoveryPollInterval = 5 * time.Millisecond
	os.Exit(m.Run())
}

// containerStatusHandler returns a GET handler for /{containerID} that:
//   - emits FINISHED for the first call (waitForContainerReady, pre-publish),
//   - then emits FINISHED for `finishedRepeats` more calls (recovery polling
//     observing FINISHED before the status row flips),
//   - then emits `terminalStatus` for every subsequent call.
//
// Pass finishedRepeats=0 to flip to the terminal status immediately on the
// first recovery-side read (the common "post got created promptly" case).
// Pass finishedRepeats > maxRecoveryStatusPolls to exhaust the recovery
// poll budget while staying FINISHED.
func containerStatusHandler(finishedRepeats int, terminalStatus string) (http.HandlerFunc, *int32) {
	var calls int32
	h := func(w http.ResponseWriter, r *http.Request) {
		n := int(atomic.AddInt32(&calls, 1))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		// Call 1 = waitForContainerReady (must see FINISHED to proceed).
		// Calls 2..(1+finishedRepeats) = additional recovery polls that
		// still see FINISHED.
		// Calls after that = terminalStatus.
		if n <= 1+finishedRepeats {
			_, _ = w.Write([]byte(`{"id":"the_container","status":"FINISHED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"the_container","status":"` + terminalStatus + `"}`))
	}
	return h, &calls
}

// TestCreateCarouselPost_RecoversAfterCode10 verifies the documented Meta
// quirk where /threads_publish returns HTTP 500 + code 10 even though the
// carousel was actually published. After the failed publish, recovery should:
//   - confirm container status is PUBLISHED (polling-tolerant),
//   - locate the published post via /me/threads using a content-based
//     matcher (NOT the child container IDs we passed, which won't appear in
//     the read-side `children` field — see makeCarouselMatcher's doc),
//   - return that post as if the publish had succeeded.
//
// The mock returns child IDs of the form "published_child_*" to make the
// container-vs-post-ID distinction explicit; a regression that re-introduces
// children-ID set matching would fail to recover here.
func TestCreateCarouselPost_RecoversAfterCode10(t *testing.T) {
	var publishAttempts int32
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			atomic.AddInt32(&publishAttempts, 1)
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
			// Production-shape mock: children carry POST IDs of the
			// individual published children, not the container IDs we
			// passed in CreateCarouselPost.Children. We must match by
			// content + count, not by ID set equality.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"recovered_post","media_type":"CAROUSEL_ALBUM","text":"my carousel","topic_tag":"F1Threads","is_reply":false,"children":{"data":[{"id":"published_child_a"},{"id":"published_child_b"}]}}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "my carousel",
		Children: []string{"child_1", "child_2"},
		TopicTag: "F1Threads",
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

// TestCreateCarouselPost_NoRecoveryWhenContainerNeverPublishes verifies that
// when the container is NEVER seen in PUBLISHED state (stays FINISHED past
// the poll budget), recovery exhausts polls and surfaces the original
// publish error.
func TestCreateCarouselPost_NoRecoveryWhenContainerNeverPublishes(t *testing.T) {
	// Stay FINISHED for far more attempts than the poll budget.
	containerStatus, _ := containerStatusHandler(maxRecoveryStatusPolls+2, "FINISHED")
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
		t.Fatal("expected the original publish error to surface when container never reaches PUBLISHED")
	}
	if !strings.Contains(err.Error(), "threads api error 10") {
		t.Errorf("expected wrapped code 10 error, got: %v", err)
	}
}

// TestCreateCarouselPost_NoRecoveryWhenContainerErrored confirms that a
// terminal failure status (ERROR) short-circuits the status-polling loop
// rather than burning the full poll budget.
func TestCreateCarouselPost_NoRecoveryWhenContainerErrored(t *testing.T) {
	containerStatus, statusCalls := containerStatusHandler(0, "ERROR")
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
		t.Fatal("expected the original publish error to surface for ERROR container status")
	}
	// 1 call from waitForContainerReady + 1 call from recovery (saw ERROR,
	// short-circuited). No additional polls.
	if got := atomic.LoadInt32(statusCalls); got > 2 {
		t.Errorf("recovery should short-circuit on terminal ERROR; status was queried %d times", got)
	}
}

// TestRecovery_PollsContainerStatus exercises the bounded-polling path:
// status reads return FINISHED a few times before flipping to PUBLISHED.
// Without this loop, a race between Meta returning code 10 and the
// container status row flipping would mis-classify a successful publish.
func TestRecovery_PollsContainerStatus(t *testing.T) {
	// 2 recovery-side FINISHED reads, then PUBLISHED.
	containerStatus, _ := containerStatusHandler(2, "PUBLISHED")
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
                {"id":"recovered_post","media_type":"IMAGE","is_reply":false,"text":"the caption","topic_tag":"Hello"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "the caption",
		ImageURL: "https://example.com/img.jpg",
		TopicTag: "Hello",
	})
	if err != nil {
		t.Fatalf("expected recovery to succeed after status polling, got: %v", err)
	}
	if post.ID != "recovered_post" {
		t.Fatalf("expected recovered_post, got %s", post.ID)
	}
}

// TestRecovery_PollsUserPostsList exercises the list-polling path: the
// first /me/threads call returns no match (index lag), a subsequent call
// returns the published post. This is the failure mode observed in
// production with v1.9.3 — recovery was correct, but a single immediate
// list read missed the post.
func TestRecovery_PollsUserPostsList(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
	var listCalls int32
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
			n := atomic.AddInt32(&listCalls, 1)
			if n == 1 {
				// First read: index hasn't seen the post yet.
				_, _ = w.Write([]byte(`{"data":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[
                {"id":"recovered_post","media_type":"IMAGE","is_reply":false,"text":"the caption","topic_tag":"Hello"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "the caption",
		ImageURL: "https://example.com/img.jpg",
		TopicTag: "Hello",
	})
	if err != nil {
		t.Fatalf("expected recovery after list-polling to succeed, got: %v", err)
	}
	if post.ID != "recovered_post" {
		t.Fatalf("expected recovered_post, got %s", post.ID)
	}
	if got := atomic.LoadInt32(&listCalls); got < 2 {
		t.Errorf("expected at least 2 list calls (first empty, second matched), got %d", got)
	}
}

// TestCreateCarouselPost_FailClosedOnMultipleMatches: matchers must be
// unique per request, so >1 match means we'd be guessing. Confirm we fail
// closed rather than returning the wrong post.
func TestCreateCarouselPost_FailClosedOnMultipleMatches(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
			// Two same-text, same-count, same-tag carousel posts.
			// Shouldn't happen in practice — exercise the fail-closed
			// branch defensively.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"match_a","media_type":"CAROUSEL_ALBUM","text":"carousel","topic_tag":"T","is_reply":false,"children":{"data":[{"id":"a"},{"id":"b"}]}},
                {"id":"match_b","media_type":"CAROUSEL_ALBUM","text":"carousel","topic_tag":"T","is_reply":false,"children":{"data":[{"id":"c"},{"id":"d"}]}}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateCarouselPost(context.Background(), &CarouselPostContent{
		Text:     "carousel",
		Children: []string{"child_1", "child_2"},
		TopicTag: "T",
	})
	if err == nil {
		t.Fatal("expected error on ambiguous match (>1)")
	}
}

// TestCreateImagePost_RecoversReplyByParentIDAndText covers the single-image
// reply case where the caller supplies non-empty text. Parent ID alone is
// not unique across replies, so the matcher also requires text equality.
func TestCreateImagePost_RecoversReplyByParentIDAndText(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
			// Production-shape mock: PostExtendedFields returns `replied_to`
			// (object), not `reply_to` (string).
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

// TestCreateImagePost_BlankTextReplyFailsClosed: blank-text non-quote
// replies have no unique discriminator beyond parent ID; recovery must
// not guess.
func TestCreateImagePost_BlankTextReplyFailsClosed(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
	})
	if err == nil {
		t.Fatal("expected fail-closed for blank-text non-quote image reply")
	}
}

// TestCreateImagePost_BlankTextBlankTagRootFailsClosed: a non-reply
// image-only post with empty text AND empty topic_tag has no discriminator
// — matching by media_type + !is_reply alone would accept any prior
// unrelated image. Recovery must fail closed.
func TestCreateImagePost_BlankTextBlankTagRootFailsClosed(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
			// A prior unrelated image post in the recovery window. Without
			// the discriminator rule, matching by media_type+!is_reply
			// alone would return this and the bot could chain off the
			// wrong post ID.
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[
                {"id":"prior_image","media_type":"IMAGE","is_reply":false,"text":"","topic_tag":""}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	_, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		ImageURL: "https://example.com/img.jpg",
		// Text, TopicTag, QuotedPostID all empty — no discriminator.
	})
	if err == nil {
		t.Fatal("expected fail-closed for blank-text + blank-tag image root (no unique signal)")
	}
}

// TestCreateImagePost_QuotePostNotMistakenForRegular verifies the quote-state
// gate: if the caller asked for a non-quote image but a same-text quote post
// shows up in the recovery window, the matcher must NOT pick it.
func TestCreateImagePost_QuotePostNotMistakenForRegular(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
	})
	if err == nil {
		t.Fatal("expected fail-closed when only candidate is a quote post and caller asked for non-quote")
	}
}

// TestCreateImagePost_RecoversRootByTopicTag covers the non-reply image case
// where two posts in the recovery window share the same text and differ only
// by topic_tag.
func TestCreateImagePost_RecoversRootByTopicTag(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
                {"id":"wrong_topic_post","media_type":"IMAGE","is_reply":false,"text":"sunset","topic_tag":"OtherTopic"},
                {"id":"recovered_post","media_type":"IMAGE","is_reply":false,"text":"sunset","topic_tag":"GoldenHour"}
            ]}`))
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))
	post, err := client.CreateImagePost(context.Background(), &ImagePostContent{
		Text:     "sunset",
		ImageURL: "https://example.com/img.jpg",
		TopicTag: "GoldenHour",
	})
	if err != nil {
		t.Fatalf("expected recovery via text+topic_tag match, got: %v", err)
	}
	if post == nil || post.ID != "recovered_post" {
		t.Fatalf("expected recovered_post, got %#v", post)
	}
}

// TestCreateImagePost_RecoversRootByText covers the non-reply image case
// where text is set but topic_tag is empty. Empty-tag matches empty-tag.
func TestCreateImagePost_RecoversRootByText(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
// end-to-end. Two same-text posts share the recovery window and only differ
// by topic_tag.
func TestCreateTextPost_RecoversAfterCode10(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
// code-10 pattern. For other publish failures (e.g. validation code 100),
// recovery must NOT issue extra Meta calls — the /me/threads lookup is the
// signal that recovery actually fired.
func TestRecovery_NotTriggeredForNonCode10(t *testing.T) {
	var threadsListCalls int32
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads_publish"):
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"Param creation_id must be a number","type":"THApiException","code":100,"fbtrace_id":"test"}}`))
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"image_container"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/image_container"):
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"image_container","status":"FINISHED"}`))
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/12345/threads"):
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

// TestRecovery_ContextCancellationPropagates verifies that if the caller's
// context is cancelled (or deadline-exceeds) DURING recovery polling, the
// caller-facing error is the context error — not the wrapped original
// publish error. Downstream retry/classification code relies on
// errors.Is(err, context.Canceled / DeadlineExceeded) to distinguish a
// real publish failure from "we ran out of time during recovery."
func TestRecovery_ContextCancellationPropagates(t *testing.T) {
	// Hold poll interval long enough that the caller's short deadline
	// fires while we're sleeping between polls. The default 5ms set in
	// TestMain would race the deadline; override for this test only.
	orig := recoveryPollInterval
	recoveryPollInterval = 50 * time.Millisecond
	t.Cleanup(func() { recoveryPollInterval = orig })

	// Container stays FINISHED forever — polling never reaches PUBLISHED
	// nor a terminal failure, so it WILL exhaust the budget. Caller's
	// short deadline should fire well before we hit the budget.
	containerStatus, _ := containerStatusHandler(maxRecoveryStatusPolls+5, "FINISHED")
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
		default:
			http.NotFound(w, r)
		}
	}

	client := testClient(t, http.HandlerFunc(handler))

	// 30ms deadline << poll interval. Recovery should sleep, see ctx
	// done, and bail with ctx.Err().
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := client.CreateImagePost(ctx, &ImagePostContent{
		Text:     "hello",
		ImageURL: "https://example.com/img.jpg",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded to propagate; got %v (Is(DeadlineExceeded)=%v)",
			err, errors.Is(err, context.DeadlineExceeded))
	}
	// And specifically NOT the wrapped publish error.
	if strings.Contains(err.Error(), "failed to publish image post") {
		t.Errorf("expected raw ctx error, got wrapped publish error: %v", err)
	}
}

// TestRecovery_NotRecoveredErrorSurfacesOriginal verifies that when recovery
// is attempted but fails to find a matching post (after exhausting list
// polls), the caller-facing path surfaces the original publish error —
// not errPublishNotRecovered.
func TestRecovery_NotRecoveredErrorSurfacesOriginal(t *testing.T) {
	containerStatus, _ := containerStatusHandler(0, "PUBLISHED")
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
			_, _ = w.Write([]byte(`{"data":[]}`)) // never indexed
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
