package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.CommentKind;
import ai.pipestream.confluence.v1.ContentRestrictionState;
import ai.pipestream.confluence.v1.DatabaseFieldType;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.SecurityCompleteness;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end crawls against the fake: a full sweep into the in-memory sink,
 * the space allowlist, and the incremental stop-at-cursor walk over
 * {@code sort=-modified-date}.
 */
class ConfluenceCrawlerTest {

    private static final Instant T1 = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant T2 = Instant.parse("2024-03-02T00:00:00Z");
    private static final Instant T3 = Instant.parse("2024-03-03T00:00:00Z");

    private FakeConfluenceServer fake;
    private InMemoryChangeSink sink;

    @BeforeEach
    void startFake() throws Exception {
        fake = FakeConfluenceServer.start();
        sink = new InMemoryChangeSink();
    }

    @AfterEach
    void stopFake() {
        fake.close();
    }

    private ConfluenceCrawler crawler(ConfluenceConnectorConfig config) {
        return new ConfluenceCrawler(config,
                new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                        Duration.ZERO),
                sink);
    }

    private ConfluenceConnectorConfig config(String... spaces) {
        return ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .spaces(spaces)
                .build();
    }

    /** Stubs one ENG space with one page, one blog post, and full sub-entities. */
    private void stubWorkspace() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.spacePropertyJson("sp1", "custom-theme", "\"blue\"")));

        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString())));
        fake.stub("/wiki/api/v2/pages/200/footer-comments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.footerCommentJson("c1", "200")));
        fake.stub("/wiki/api/v2/pages/200/inline-comments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.inlineCommentJson("c2", "200")));
        fake.stub("/wiki/api/v2/pages/200/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a1", "200")));
        fake.stub("/wiki/api/v2/pages/200/labels",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.labelJson("l1", "design")));
        fake.stub("/wiki/api/v2/pages/200/properties",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.propertyJson("p1", "editor", "\"v2\"")));

        fake.stub("/wiki/api/v2/blogposts",
                ConfluenceFixtures.blogPostListJson(null,
                        ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                                T1.toString())));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/blogposts/300/" + sub, ConfluenceFixtures.emptyListJson());
        }
    }

    @Test
    void fullCrawlEmitsEveryEntityAndOneSnapshotPerSpace() throws Exception {
        stubWorkspace();

        crawler(config()).crawl();

        List<ConfluenceChange> changes = sink.changes();
        assertThat(changes).allSatisfy(change -> {
            assertThat(change.getSource()).isEqualTo(ChangeSource.CHANGE_SOURCE_CRAWL);
            assertThat(change.getOperation().name()).isEqualTo("CHANGE_OPERATION_UPSERT");
            assertThat(change.getChangeId()).isNotBlank();
            assertThat(change.getCursor()).isNotBlank();
        });
        assertThat(changes).extracting(c -> c.getEntity().getEntityCase())
                .containsExactlyInAnyOrder(
                        ConfluenceEntity.EntityCase.SPACE,
                        ConfluenceEntity.EntityCase.SPACE_PROPERTY,
                        ConfluenceEntity.EntityCase.PAGE,
                        ConfluenceEntity.EntityCase.COMMENT,
                        ConfluenceEntity.EntityCase.COMMENT,
                        ConfluenceEntity.EntityCase.ATTACHMENT,
                        ConfluenceEntity.EntityCase.LABEL,
                        ConfluenceEntity.EntityCase.CONTENT_PROPERTY,
                        ConfluenceEntity.EntityCase.BLOG_POST,
                        // The per-owner security marker of the page and the blog post.
                        ConfluenceEntity.EntityCase.RESTRICTION_STATE,
                        ConfluenceEntity.EntityCase.RESTRICTION_STATE);
        // No security route is stubbed: 404 everywhere = not served = complete.
        assertThat(changes).filteredOn(c -> c.getEntity().hasSpace() || c.getEntity().hasPage()
                        || c.getEntity().hasBlogPost() || c.getEntity().hasRestrictionState())
                .allSatisfy(c -> assertThat(c.getEntity().getSecurityCompleteness())
                        .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE));
        assertThat(changes).filteredOn(c -> c.getEntity().hasComment())
                .allSatisfy(c -> assertThat(c.getEntity().getSecurityCompleteness())
                        .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_UNSPECIFIED));

        ConfluenceEntity page = changes.stream()
                .map(ConfluenceChange::getEntity)
                .filter(e -> e.getEntityCase() == ConfluenceEntity.EntityCase.PAGE)
                .findFirst().orElseThrow();
        assertThat(page.getEntityId()).isEqualTo("200");
        assertThat(page.getPage().getBody().getStorage().getValue())
                .isEqualTo("<p>Hello Design Doc</p>");
        assertThat(page.getPage().getWebUrl())
                .isEqualTo(fake.baseUrl() + "/spaces/ENG/pages/200/Design+Doc");
        assertThat(page.getIngestedAt().getSeconds()).isGreaterThan(0);

        assertThat(sink.snapshots()).hasSize(1);
        ConfluenceSnapshot snapshot = sink.snapshots().get(0);
        assertThat(snapshot.getSpaceKey()).isEqualTo("ENG");
        assertThat(snapshot.getEntityCountsMap())
                .containsEntry("page", 1L)
                .containsEntry("blogpost", 1L)
                .containsEntry("comment", 2L)
                .containsEntry("attachment", 1L)
                .containsEntry("label", 1L)
                .containsEntry("content_property", 1L)
                .containsEntry("space_property", 1L);
        // The resume cursor is the newest modification observed (the page at T2).
        assertThat(Instant.parse(snapshot.getCursor())).isEqualTo(T2);
        assertThat(snapshot.hasCompletedAt()).isTrue();
    }

    @Test
    void fullCrawlEmitsSecurityTrimmingDataTreeContentAndDatabaseRows() throws Exception {
        stubWorkspace();
        // Space-level: permission assignments; role assignments are refused with a 403.
        fake.stub("/wiki/api/v2/spaces/100/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"sp-1\",\"principal\":{\"type\":\"group\",\"id\":\"g1\"},"
                        + "\"operation\":{\"key\":\"read\",\"targetType\":\"space\"}}"));
        fake.stub("/wiki/api/v2/spaces/100/role-assignments", new FakeConfluenceServer.Stub(403,
                "{\"message\":\"role-based access is not enabled\"}", Map.of()));
        // Page-level restriction state and direct permissions.
        fake.stub("/wiki/api/v2/pages/200/restriction-state", "{\"state\":\"VIEW_RESTRICTED\"}");
        fake.stub("/wiki/api/v2/pages/200/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"operation\":\"read\",\"principal\":{\"principalType\":\"user\","
                        + "\"principalId\":\"abc\"}}",
                "{\"operation\":\"update\",\"principal\":{\"principalType\":\"group\","
                        + "\"principalId\":\"g1\"}}"));
        // The page's tree: a folder (holding a whiteboard and the page itself) and a database.
        fake.stub("/wiki/api/v2/pages/200/direct-children", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"f1\",\"type\":\"folder\",\"title\":\"Folder\",\"status\":\"current\"}",
                "{\"id\":\"d1\",\"type\":\"database\",\"title\":\"Tracker\",\"status\":\"current\"}"));
        fake.stub("/wiki/api/v2/folders/f1", "{\"id\":\"f1\",\"type\":\"folder\",\"title\":\"Folder\","
                + "\"spaceId\":\"100\",\"status\":\"current\"}");
        fake.stub("/wiki/api/v2/folders/f1/direct-children", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"w1\",\"type\":\"whiteboard\",\"title\":\"Board\",\"status\":\"current\"}",
                "{\"id\":\"200\",\"type\":\"page\",\"title\":\"Design Doc\",\"status\":\"current\"}",
                "{\"id\":\"e1\",\"type\":\"embed\",\"title\":\"Link\",\"status\":\"current\"}"));
        fake.stub("/wiki/api/v2/whiteboards/w1", "{\"id\":\"w1\",\"type\":\"whiteboard\","
                + "\"title\":\"Board\",\"spaceId\":\"100\",\"status\":\"current\"}");
        fake.stub("/wiki/api/v2/whiteboards/w1/restriction-state", "{\"state\":\"OPEN\"}");
        fake.stub("/wiki/api/v2/embeds/e1", "{\"id\":\"e1\",\"type\":\"embed\",\"title\":\"Link\","
                + "\"spaceId\":\"100\",\"status\":\"current\",\"embedUrl\":\"https://x\"}");
        fake.stub("/wiki/api/v2/databases/d1", "{\"id\":\"d1\",\"type\":\"database\","
                + "\"title\":\"Tracker\",\"spaceId\":\"100\",\"status\":\"current\"}");
        fake.stub("/wiki/api/v2/databases/d1/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"operation\":\"read\",\"principal\":{\"principalType\":\"group\","
                        + "\"principalId\":\"g2\"}}"));
        fake.stub("/wiki/api/v2/databases/d1/fields", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"fld1\",\"name\":\"Done\",\"type\":\"checkbox\"}"));
        fake.stub("/wiki/api/v2/databases/d1/entries", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"row1\",\"databaseId\":\"d1\",\"values\":{\"fld1\":{\"type\":\"checkbox\","
                        + "\"value\":true}}}"));

        crawler(config()).crawl();

        Map<String, ConfluenceEntity> byId = new java.util.HashMap<>();
        for (ConfluenceChange change : sink.changes()) {
            assertThat(byId.put(change.getEntity().getEntityId(), change.getEntity()))
                    .as("entity_id %s is unique", change.getEntity().getEntityId()).isNull();
        }
        assertThat(byId.get("space:100/permission/sp-1").getSpacePermissionAssignment()
                .getPrincipal().getId()).isEqualTo("g1");
        assertThat(byId.get("page:200/restriction-state").getRestrictionState().getState())
                .isEqualTo(ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED);
        assertThat(byId).containsKeys("page:200/permission/read/user/abc",
                "page:200/permission/update/group/g1", "database:d1/permission/read/group/g2",
                "whiteboard:w1/restriction-state");
        assertThat(byId.get("f1").getEntityCase()).isEqualTo(ConfluenceEntity.EntityCase.FOLDER);
        assertThat(byId.get("w1").getWhiteboard().getTitle()).isEqualTo("Board");
        assertThat(byId.get("e1").getSmartLink().getEmbedUrl()).isEqualTo("https://x");
        assertThat(byId.get("d1").getDatabase().getTitle()).isEqualTo("Tracker");
        assertThat(byId.get("database:d1/field/fld1").getDatabaseFieldDefinition().getType())
                .isEqualTo(DatabaseFieldType.DATABASE_FIELD_TYPE_CHECKBOX);
        assertThat(byId.get("database:d1/entry/row1").getDatabaseEntry().getValuesMap()
                .get("fld1").getCheckbox()).isTrue();
        // The page reached again through the folder is not emitted twice.
        assertThat(sink.changes()).filteredOn(c -> c.getEntity().hasPage()).hasSize(1);
        // Comments carry the kind of the route they were read from.
        assertThat(byId.get("c1").getComment().getKind()).isEqualTo(CommentKind.COMMENT_KIND_FOOTER);
        assertThat(byId.get("c2").getComment().getKind()).isEqualTo(CommentKind.COMMENT_KIND_INLINE);
        // A 403 on a security route fails closed: the space is UNAVAILABLE, the crawl goes on.
        assertThat(byId.get("100").getSecurityCompleteness())
                .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_UNAVAILABLE);
        assertThat(byId.get("200").getSecurityCompleteness())
                .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE);
        assertThat(byId.get("page:200/restriction-state").getSecurityCompleteness())
                .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE);
        assertThat(byId.get("e1").getSecurityCompleteness())
                .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_UNSPECIFIED);
        // Smart Links have no permission routes; role assignments were refused yet the crawl finished.
        assertThat(fake.requestsTo("/wiki/api/v2/embeds/e1/permissions")).isEmpty();
        assertThat(fake.requestsTo("/wiki/api/v2/spaces/100/role-assignments")).hasSize(1);
        assertThat(sink.snapshots()).singleElement().satisfies(snapshot ->
                assertThat(snapshot.getEntityCountsMap())
                        .containsEntry("space_permission_assignment", 1L)
                        // page 200, blog post 300, folder f1, whiteboard w1, database d1
                        .containsEntry("restriction_state", 5L)
                        .containsEntry("content_permission", 3L)
                        .containsEntry("folder", 1L)
                        .containsEntry("whiteboard", 1L)
                        .containsEntry("smart_link", 1L)
                        .containsEntry("database", 1L)
                        .containsEntry("database_field", 1L)
                        .containsEntry("database_entry", 1L)
                        .doesNotContainKey("space_role_assignment"));
        sink.changes().forEach(change -> ConfluenceValidator.create().requireValid(change));
    }

    @Test
    void roleAssignmentsAreEmittedOnRoleBasedTenants() throws Exception {
        stubWorkspace();
        fake.stub("/wiki/api/v2/spaces/100/role-assignments", ConfluenceFixtures.listJson(null, "",
                "{\"principal\":{\"principalType\":\"ACCESS_CLASS\",\"principalId\":"
                        + "\"authenticated-users\"},\"roleId\":\"r1\"}"));

        crawler(config()).crawl();

        assertThat(sink.changes()).filteredOn(c -> c.getEntity().hasSpaceRoleAssignment())
                .singleElement().satisfies(c -> {
                    assertThat(c.getEntity().getEntityId())
                            .isEqualTo("space:100/role-assignment/r1/access_class/authenticated-users");
                    assertThat(c.getEntity().getSpaceRoleAssignment().getRoleId()).isEqualTo("r1");
                });
    }

    @Test
    void aServerErrorOnASecurityRouteStillFailsTheCrawl() {
        stubWorkspace();
        fake.stub("/wiki/api/v2/spaces/100/permissions",
                new FakeConfluenceServer.Stub(500, "{\"message\":\"boom\"}", Map.of()));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> crawler(config()).crawl())
                .isInstanceOf(ConfluenceClient.ConfluenceApiException.class);
    }

    @Test
    void spaceAllowlistSkipsOtherSpaces() throws Exception {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());

        crawler(config("ENG")).crawl();

        assertThat(sink.snapshots()).hasSize(1);
        // The allowlist reaches the API as the keys filter too.
        assertThat(fake.requestsTo("/wiki/api/v2/spaces").get(0).query()).contains("keys=ENG");
    }

    @Test
    void incrementalWalksNewestFirstAndStopsAtCursor() throws Exception {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        // Newest-first, as sort=-modified-date promises: T3, T2, T1.
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("203", "100", "Newest", T3.toString()),
                        ConfluenceFixtures.pageJson("202", "100", "Middle", T2.toString()),
                        ConfluenceFixtures.pageJson("201", "100", "Oldest", T1.toString())));
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/203/" + sub, ConfluenceFixtures.emptyListJson());
        }

        String newCursor = crawler(config()).crawlIncremental(T2.toString());

        // Only the page newer than the cursor was emitted; T2 and older stop the walk.
        List<ConfluenceChange> changes = sink.changes();
        assertThat(changes).allSatisfy(c -> assertThat(c.getCursor()).isEqualTo(T2.toString()));
        List<ConfluenceChange> pages = changes.stream().filter(c -> c.getEntity().hasPage()).toList();
        assertThat(pages).hasSize(1);
        ConfluenceChange change = pages.get(0);
        assertThat(change.getSource()).isEqualTo(ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL);
        assertThat(change.getCursor()).isEqualTo(T2.toString());
        assertThat(change.getEntity().getEntityCase()).isEqualTo(ConfluenceEntity.EntityCase.PAGE);
        assertThat(change.getEntity().getEntityId()).isEqualTo("203");
        assertThat(Instant.parse(newCursor)).isEqualTo(T3);

        // The incremental pass asked for newest-first ordering.
        assertThat(fake.requestsTo("/wiki/api/v2/pages").get(0).query())
                .contains("sort=-modified-date");

        // Security does not bump versions: every unchanged page's restriction state is
        // re-read through a metadata-only walk (no bodies, no comments or attachments).
        assertThat(changes).filteredOn(c -> c.getEntity().hasRestrictionState())
                .extracting(c -> c.getEntity().getEntityId())
                .containsExactlyInAnyOrder("page:203/restriction-state",
                        "page:202/restriction-state", "page:201/restriction-state");
        assertThat(fake.requestsTo("/wiki/api/v2/pages")).hasSize(2);
        assertThat(fake.requestsTo("/wiki/api/v2/pages").get(1).query())
                .doesNotContain("body-format");
        assertThat(fake.requestsTo("/wiki/api/v2/pages/202/restriction-state")).hasSize(1);
        assertThat(fake.requestsTo("/wiki/api/v2/pages/202/permissions")).hasSize(1);
        assertThat(fake.requestsTo("/wiki/api/v2/pages/202/direct-children")).hasSize(1);
        assertThat(fake.requestsTo("/wiki/api/v2/pages/202/footer-comments")).isEmpty();
        // The space is re-emitted with its re-read security marker.
        assertThat(changes).filteredOn(c -> c.getEntity().hasSpace()).singleElement()
                .satisfies(c -> assertThat(c.getEntity().getSecurityCompleteness())
                        .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE));
    }

    @Test
    void incrementalRevisitsTheTreeUnderUnchangedPagesAndFailsClosed() throws Exception {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("202", "100", "Middle", T1.toString())));
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        // The unchanged page's permissions: the second page answers 403.
        fake.stub("/wiki/api/v2/pages/202/permissions", ConfluenceFixtures.listJson("p2",
                "/wiki/api/v2/pages/202/permissions",
                "{\"operation\":\"read\",\"principal\":{\"principalType\":\"user\","
                        + "\"principalId\":\"abc\"}}"));
        fake.stub("/wiki/api/v2/pages/202/permissions?cursor=p2",
                new FakeConfluenceServer.Stub(403, "{}", Map.of()));
        // Under it: a whiteboard changed after the cursor and a folder that did not.
        fake.stub("/wiki/api/v2/pages/202/direct-children", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"w1\",\"type\":\"whiteboard\",\"status\":\"current\"}",
                "{\"id\":\"f1\",\"type\":\"folder\",\"status\":\"current\"}"));
        fake.stub("/wiki/api/v2/whiteboards/w1", "{\"id\":\"w1\",\"type\":\"whiteboard\","
                + "\"title\":\"Board\",\"spaceId\":\"100\",\"status\":\"current\","
                + "\"version\":{\"number\":2,\"createdAt\":\"" + T3 + "\"}}");
        fake.stub("/wiki/api/v2/folders/f1", "{\"id\":\"f1\",\"type\":\"folder\","
                + "\"title\":\"Folder\",\"spaceId\":\"100\",\"status\":\"current\","
                + "\"version\":{\"number\":1,\"createdAt\":\"" + T1 + "\"}}");

        crawler(config()).crawlIncremental(T2.toString());

        Map<String, ConfluenceEntity> byId = new java.util.HashMap<>();
        sink.changes().forEach(c -> byId.put(c.getEntity().getEntityId(), c.getEntity()));
        // The partial permission list is kept but cannot pass for complete.
        assertThat(byId).containsKey("page:202/permission/read/user/abc");
        assertThat(byId.get("page:202/restriction-state").getSecurityCompleteness())
                .isEqualTo(SecurityCompleteness.SECURITY_COMPLETENESS_UNAVAILABLE);
        assertThat(byId).doesNotContainKey("202");
        // The changed whiteboard is emitted; the unchanged folder only has its security re-read.
        assertThat(byId.get("w1").getWhiteboard().getTitle()).isEqualTo("Board");
        assertThat(byId).doesNotContainKey("f1");
        assertThat(byId).containsKeys("whiteboard:w1/restriction-state", "folder:f1/restriction-state");
        assertThat(fake.requestsTo("/wiki/api/v2/folders/f1/direct-children")).hasSize(1);
    }

    @Test
    void aRepeatedNextLinkFailsTheWalkInsteadOfSpinning() {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.spaceListJson("again",
                ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces?cursor=again", ConfluenceFixtures.spaceListJson("again"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> crawler(config()).crawl())
                .isInstanceOf(ConfluenceClient.RepeatedCursorException.class);
    }

    @Test
    void incrementalWithNothingNewKeepsTheCursor() throws Exception {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("202", "100", "Middle", T2.toString())));
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());

        String newCursor = crawler(config()).crawlIncremental(T2.toString());

        // No content is re-emitted; only the space and the page's security marker are.
        assertThat(sink.changes()).extracting(c -> c.getEntity().getEntityId())
                .containsExactlyInAnyOrder("100", "page:202/restriction-state");
        assertThat(Instant.parse(newCursor)).isEqualTo(T2);
    }
}
