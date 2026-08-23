package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ContentProperty;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.PropertyKey;
import ai.pipestream.confluence.v1.Redaction;
import ai.pipestream.confluence.v1.User;
import ai.pipestream.confluence.v1.Version;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the programmatic validator applies the same rule families the
 * original proto options encoded: identity, numeric floors, and cross-field
 * facts, both the violating and the passing shape.
 */
class ConfluenceValidationTest {

    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected no violations")
                .isEmpty();
    }

    @Test
    void pageIdentityIsRequired() {
        assertViolation(Page.getDefaultInstance(), "id", "required");
        assertViolation(Page.getDefaultInstance(), "space_id", "required");
        assertValid(Page.newBuilder().setId("123").setSpaceId("456").build());
    }

    @Test
    void versionNumbersAreNeverNegative() {
        assertValid(Version.getDefaultInstance());
        assertViolation(Version.newBuilder().setNumber(-1).build(), "number", "int32.gte");
        assertValid(Version.newBuilder().setNumber(1).build());
    }

    @Test
    void userEmailMustBeAnEmailWhenPresent() {
        User valid = User.newBuilder().setAccountId("abc").build();
        assertValid(valid);
        assertValid(valid.toBuilder().setEmail("kim@example.com").build());
        assertViolation(valid.toBuilder().setEmail("not-an-email").build(),
                "email", "user.email_format");
        assertViolation(User.getDefaultInstance(), "account_id", "required");
    }

    @Test
    void redactionRangeAndPointerRules() {
        Redaction valid = Redaction.newBuilder()
                .setPointer("/body/storage")
                .setFrom(3).setTo(9)
                .setRedactionId("6cd7e1d0-3f7b-4a86-9b0f-6f34e0a2b8d1")
                .build();
        assertValid(valid);
        assertViolation(Redaction.getDefaultInstance(), "pointer", "required");
        assertViolation(valid.toBuilder().setFrom(10).setTo(4).build(),
                "", "redaction.range");
        assertViolation(valid.toBuilder().setRedactionId("not-a-uuid").build(),
                "redaction_id", "redaction.id_uuid");
        assertValid(valid.toBuilder().clearRedactionId().build());
    }

    @Test
    void customPropertyKeyIsExplicitBothWays() {
        ContentProperty custom = ContentProperty.newBuilder()
                .setId("p1")
                .setKey(PropertyKey.PROPERTY_KEY_CUSTOM)
                .setCustomKey("team.owner")
                .build();
        assertValid(custom);
        assertViolation(custom.toBuilder().clearCustomKey().build(),
                "", "property.custom_key");
        assertViolation(ContentProperty.newBuilder()
                        .setId("p2")
                        .setKey(PropertyKey.PROPERTY_KEY_EDITOR)
                        .setCustomKey("stray")
                        .build(),
                "", "property.custom_key");
    }

    @Test
    void populatedBodyMustDeclareItsFormat() {
        assertValid(BodyType.getDefaultInstance());
        assertViolation(BodyType.newBuilder().setValue("<p>hi</p>").build(),
                "", "body_type.format_declared");
        assertValid(BodyType.newBuilder()
                .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                .setValue("<p>hi</p>")
                .build());
    }

    @Test
    void upsertChangesMustCarryTheirEntity() {
        ConfluenceChange delete = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build();
        assertValid(delete);
        assertViolation(delete.toBuilder()
                        .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                        .build(),
                "", "change.upsert_has_entity");
        assertValid(ConfluenceChange.newBuilder()
                .setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("page:123")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1_753_000_000)))
                .build());
    }

    @Test
    void attachmentAndBlogIdentity() {
        assertViolation(ai.pipestream.confluence.v1.Attachment.getDefaultInstance(), "id", "required");
        assertViolation(ai.pipestream.confluence.v1.Attachment.newBuilder()
                        .setId("a1")
                        .setFileSize(-1)
                        .build(),
                "file_size", "int64.gte");
        assertValid(ai.pipestream.confluence.v1.Attachment.newBuilder().setId("a1").build());
        assertViolation(ai.pipestream.confluence.v1.BlogPost.getDefaultInstance(), "id", "required");
        assertViolation(ai.pipestream.confluence.v1.BlogPost.getDefaultInstance(), "space_id", "required");
        assertValid(ai.pipestream.confluence.v1.BlogPost.newBuilder()
                .setId("300")
                .setSpaceId("100")
                .build());
    }
}
