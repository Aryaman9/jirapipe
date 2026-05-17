package com.jirapipe.feedback;

import com.jirapipe.feedback.dto.FeedbackRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/feedback")
@Tag(name = "Feedback", description = "Resolution feedback endpoints")
public class FeedbackController {

    private final FeedbackService feedbackService;

    public FeedbackController(FeedbackService feedbackService) {
        this.feedbackService = feedbackService;
    }

    @PostMapping("/{ticketKey}")
    @Operation(summary = "Submit feedback on a ticket resolution")
    public ResponseEntity<Void> submitFeedback(@PathVariable String ticketKey,
                                                @Valid @RequestBody FeedbackRequest request) {
        feedbackService.submitFeedback(ticketKey, request);
        return ResponseEntity.ok().build();
    }
}
