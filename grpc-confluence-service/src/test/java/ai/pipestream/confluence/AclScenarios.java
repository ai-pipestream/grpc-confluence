package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.*;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.google.protobuf.Timestamp;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The access-control read steps: a parent page restricted to the caller with
 * an open child page under it, read back through GetContentAccess, and the
 * group rpcs on a group of the caller. The same steps run against the real
 * workspace and against a recording of it.
 */
final class AclScenarios {

    /** The scenario that deletes what the run created. */
    static final String CLEANUP = "cleanup";
    /** The rpcs these scenarios prove. */
    static final List<String> RPCS = List.of("GetContentAccess", "GetGroup", "ListGroupMembers",
            "ListUserGroups", "Sync");
    private static final long CALL_DEADLINE_SECONDS = 120;

    /**
     * What the steps run against.
     *
     * @param spaceKey the space everything is created in
     * @param run the title of the parent page, and the prefix of every name the run creates
     * @param secrets values that must never reach a row
     * @param tape the recording or replay the calls go through
     * @param loginEmail the email the run signs in with, or empty when replaying
     */
    record Backend(String spaceKey, String run, List<String> secrets, ExchangeTape tape, String loginEmail) {
    }

    /** One report row. */
    record Row(String rpc, String label, String outcome, String detail) {
    }

    /** One named group of steps. */
    record Scenario(String name, Runnable body) {
    }

    @FunctionalInterface
    private interface Step {
        String run() throws Exception;
    }

    private final List<Row> rows = new ArrayList<>();
    private final List<String> leftBehind = new ArrayList<>();
    private final List<String> created = new ArrayList<>();
    private final Channel channel;
    private final Backend backend;
    private Space space;
    private Page parent;
    private Page child;
    private String callerGroup;
    private final List<ConfluenceEntity> syncRecords = new ArrayList<>();

    AclScenarios(Channel channel, Backend backend) {
        this.channel = channel;
        this.backend = backend;
    }

    /** The scenario names, in the order they run; {@link #CLEANUP} is last. */
    static List<String> names() {
        return List.of("setup", "content-access", "groups", "sync", CLEANUP);
    }

    /** The scenarios in the order of {@link #names()}. */
    List<Scenario> scenarios() {
        return List.of(new Scenario("setup", this::setup),
                new Scenario("content-access", () -> {
                    if (child != null) {
                        contentAccess();
                    }
                }),
                new Scenario("groups", () -> {
                    if (parent != null) {
                        groups();
                    }
                }),
                new Scenario("sync", () -> {
                    if (child != null) {
                        sync();
                    }
                }),
                new Scenario(CLEANUP, this::cleanUp));
    }

    List<Row> rows() {
        return List.copyOf(rows);
    }

    /** The ACL and directory records the Sync step received, by entity id. */
    List<ConfluenceEntity> syncRecords() {
        return List.copyOf(syncRecords);
    }

    List<String> leftBehind() {
        return List.copyOf(leftBehind);
    }

    // ======================================================================
    // THE STEPS
    // ======================================================================

    private void setup() {
        step("CreatePage", "parent page restricted to the caller, open child page", () -> {
            space = stub().getSpace(GetSpaceRequest.newBuilder().setKey(backend.spaceKey()).build()).getSpace();
            parent = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setTitle(backend.run()).setBody(storage("<p>Parent of the ACL run.</p>")).build()).getPage();
            created.add(parent.getId());
            child = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setParentId(parent.getId()).setTitle(backend.run() + " child")
                    .setBody(storage("<p>Child of the ACL run.</p>")).build()).getPage();
            created.add(child.getId());
            ContentRef ref = page(parent.getId());
            stub().addContentPermissions(AddContentPermissionsRequest.newBuilder().setContent(ref)
                    .addPermissions(ContentPermissionGrant.newBuilder()
                            .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ)
                            .addPrincipals(self()))
                    .addPermissions(ContentPermissionGrant.newBuilder()
                            .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE)
                            .addPrincipals(self())).build());
            stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(ref)
                    .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED).build());
            return "parent " + parent.getId() + " view-restricted, child " + child.getId();
        });
    }

    private void contentAccess() {
        step("GetContentAccess", "GetContentAccess (child under a view-restricted parent)", () -> {
            GetContentAccessResponse access = stub().getContentAccess(GetContentAccessRequest.newBuilder()
                    .setContent(page(child.getId())).build());
            check(access.getSpaceId().equals(space.getId()), "space " + access.getSpaceId());
            check(access.getSpacePermissionsCount() > 0, "no space permissions");
            check(access.getCompleteness() == SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE,
                    "completeness " + access.getCompleteness());
            List<String> owners = access.getOwnersList().stream().map(o -> o.getContent().getId()).toList();
            check(owners.size() >= 2 && owners.get(0).equals(child.getId()) && owners.get(1).equals(parent.getId()),
                    "owners " + owners);
            ContentAccessOwner own = access.getOwners(0);
            check(own.getRestrictionState().getState() == ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN,
                    "child state " + own.getRestrictionState().getState());
            check(own.getPermissionsCount() == 0, "child has direct permissions");
            ContentAccessOwner restricted = access.getOwners(1);
            check(restricted.getRestrictionState().getState()
                    == ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED,
                    "parent state " + restricted.getRestrictionState().getState());
            check(restricted.getPermissionsList().stream().anyMatch(p ->
                    p.getPrincipal().getPrincipalId().equals(parent.getAuthorId())),
                    "the parent's permissions do not list the caller");
            return owners.size() + " owners, parent VIEW_RESTRICTED to the caller, "
                    + access.getSpacePermissionsCount() + " space permissions";
        });
    }

    private void groups() {
        String caller = parent.getAuthorId();
        Set<String> callerGroups = new LinkedHashSet<>();
        step("ListUserGroups", "ListUserGroups (the caller)", () -> {
            stub().listUserGroups(ListUserGroupsRequest.newBuilder().setAccountId(caller).build())
                    .forEachRemaining(r -> callerGroups.add(r.getGroup().getId()));
            check(!callerGroups.isEmpty(), "the caller is in no group");
            return callerGroups.size() + " groups";
        });
        String groupId = callerGroups.stream().sorted().findFirst().orElse(null);
        callerGroup = groupId;
        if (groupId == null) {
            for (String rpc : List.of("GetGroup", "ListGroupMembers")) {
                rows.add(new Row(rpc, rpc, "SKIPPED", "the caller's groups could not be read"));
            }
            return;
        }
        step("GetGroup", "GetGroup (a group of the caller)", () -> {
            Group group = stub().getGroup(GetGroupRequest.newBuilder().setGroupId(groupId).build()).getGroup();
            check(group.getId().equals(groupId), "id " + group.getId());
            check(!group.getName().isEmpty(), "no name");
            return "group has a name";
        });
        step("ListGroupMembers", "ListGroupMembers (same group)", () -> {
            List<User> members = new ArrayList<>();
            stub().listGroupMembers(ListGroupMembersRequest.newBuilder().setGroupId(groupId).build())
                    .forEachRemaining(r -> members.add(r.getUser()));
            check(members.stream().allMatch(u -> !u.getAccountId().isEmpty()), "a member without account id");
            check(members.stream().anyMatch(u -> u.getAccountId().equals(caller)),
                    "the members do not include the caller");
            return members.size() + " members, including the caller";
        });
    }

    private void sync() {
        step("Sync", "Sync (incremental, after granting one of the caller's groups READ on the parent)", () -> {
            String caller = parent.getAuthorId();
            ContentRef ref = page(parent.getId());
            if (callerGroup != null) {
                stub().addContentPermissions(AddContentPermissionsRequest.newBuilder().setContent(ref)
                        .addPermissions(ContentPermissionGrant.newBuilder()
                                .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ)
                                .addPrincipals(ContentPermissionPrincipal.newBuilder()
                                        .setPrincipalType(ContentPermissionPrincipalType
                                                .CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP)
                                        .setPrincipalId(callerGroup))).build());
            }
            Timestamp created = parent.getCreatedAt();
            String since = java.time.Instant.ofEpochSecond(created.getSeconds() - 120).toString();
            Map<String, ConfluenceEntity> byId = new java.util.HashMap<>();
            String cursor = null;
            backend.tape().unordered(true);
            try {
                java.util.Iterator<SyncResponse> stream = stub().sync(SyncRequest.newBuilder()
                        .addSpaceKeys(backend.spaceKey()).setSinceCursor(since).build());
                while (stream.hasNext()) {
                    SyncResponse response = stream.next();
                    if (response.hasChange()) {
                        ConfluenceEntity entity = response.getChange().getEntity();
                        byId.put(entity.getEntityId(), entity);
                    } else if (response.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR) {
                        cursor = response.getResumeCursor();
                    }
                }
            } finally {
                backend.tape().unordered(false);
            }
            syncRecords.clear();
            byId.values().stream().filter(e -> e.hasSpaceAccess() || e.hasAccessChain() || e.hasUser()
                    || e.hasGroup() || e.hasGroupMember())
                    .sorted(java.util.Comparator.comparing(ConfluenceEntity::getEntityId))
                    .forEach(syncRecords::add);
            check(cursor != null, "no resume cursor");
            ConfluenceEntity chain = byId.get("page:" + child.getId() + "/access");
            check(chain != null, "no access chain for the child");
            List<String> owners = chain.getAccessChain().getOwnersList().stream()
                    .map(o -> o.getContent().getId()).toList();
            check(chain.getAccessChain().getSpaceId().equals(space.getId()), "chain space "
                    + chain.getAccessChain().getSpaceId());
            check(owners.size() >= 2 && owners.get(0).equals(child.getId()) && owners.get(1).equals(parent.getId()),
                    "chain owners " + owners);
            check(chain.getSecurityCompleteness() == SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE,
                    "chain completeness " + chain.getSecurityCompleteness());
            ContentAccessOwner chainParent = chain.getAccessChain().getOwners(1);
            check(chainParent.getRestrictionState().getState()
                    == ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED,
                    "the chain's parent state " + chainParent.getRestrictionState().getState());
            check(chainParent.getPermissionsList().stream().anyMatch(p ->
                    p.getPrincipal().getPrincipalId().equals(caller)), "the chain's parent does not list the caller");
            ConfluenceEntity spaceAccess = byId.get("space:" + space.getId() + "/access");
            check(spaceAccess != null && spaceAccess.getSpaceAccess().getPermissionsCount() > 0,
                    "no space grants record");
            check(spaceAccess.getSecurityCompleteness() == SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE,
                    "space grants completeness " + spaceAccess.getSecurityCompleteness());
            ConfluenceEntity state = byId.get("page:" + parent.getId() + "/restriction-state");
            check(state != null && state.getRestrictionState().getState()
                    == ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED, "parent state " + state);
            List<String> parentPermissions = byId.entrySet().stream()
                    .filter(e -> e.getKey().startsWith("page:" + parent.getId() + "/permission/"))
                    .map(Map.Entry::getKey).sorted().toList();
            check(byId.entrySet().stream().anyMatch(e -> e.getValue().hasContentPermission()
                    && e.getKey().startsWith("page:" + parent.getId() + "/permission/")
                    && e.getValue().getContentPermission().getPrincipal().getPrincipalId().equals(caller)),
                    "no permission record for the caller on the parent; parent records " + parentPermissions);
            String groups = "no caller group";
            if (callerGroup != null) {
                check(byId.entrySet().stream().anyMatch(e -> e.getValue().hasContentPermission()
                        && e.getKey().startsWith("page:" + parent.getId() + "/permission/")
                        && e.getValue().getContentPermission().getPrincipal().getPrincipalId()
                                .equals(callerGroup)),
                        "no permission record for the group on the parent; parent records " + parentPermissions);
                ConfluenceEntity group = byId.get(callerGroup);
                check(group != null && group.hasGroup() && !group.getGroup().getName().isEmpty(),
                        "no group record");
                check(group.getSecurityCompleteness() == SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE,
                        "group completeness " + group.getSecurityCompleteness());
                check(byId.containsKey("group:" + callerGroup + "/member/" + caller),
                        "the group's members do not include the caller");
                groups = "the group with the caller as member";
            }
            List<User> users = byId.values().stream().filter(ConfluenceEntity::hasUser)
                    .map(ConfluenceEntity::getUser).toList();
            check(users.stream().anyMatch(u -> u.getAccountId().equals(caller)), "no user record for the caller");
            long withEmail = users.stream().filter(u -> !u.getEmail().isEmpty()).count();
            String callerEmail = users.stream().filter(u -> u.getAccountId().equals(caller)).findFirst()
                    .map(User::getEmail).orElse("");
            check(!callerEmail.isEmpty(), "the caller's user record has no email");
            String login = "";
            if (!backend.loginEmail().isEmpty()) {
                check(callerEmail.equalsIgnoreCase(backend.loginEmail()),
                        "the caller's email is not the login email");
                login = ", the caller's email is the login email";
            }
            var domains = new java.util.TreeMap<String, Integer>();
            users.stream().map(User::getEmail).filter(e -> e.indexOf('@') > 0)
                    .forEach(e -> domains.merge(domainClass(e), 1, Integer::sum));
            return "chain of " + owners.size() + " owners, parent VIEW_RESTRICTED, " + groups + ", "
                    + users.size() + " users of which " + withEmail + " with an email (domains " + domains + ")" + login;
        });
    }

    private static final java.util.Set<String> CONSUMER_DOMAINS = java.util.Set.of("gmail.com", "googlemail.com",
            "outlook.com", "hotmail.com", "live.com", "msn.com", "yahoo.com", "icloud.com", "me.com", "proton.me",
            "protonmail.com", "example.invalid");

    /** A consumer mail domain by name; any other domain only as onmicrosoft.com or custom. */
    private static String domainClass(String email) {
        String domain = email.substring(email.indexOf('@') + 1).toLowerCase(java.util.Locale.ROOT);
        if (CONSUMER_DOMAINS.contains(domain)) return domain;
        return domain.endsWith(".onmicrosoft.com") ? "onmicrosoft.com" : "custom";
    }

    private void cleanUp() {
        for (int i = created.size() - 1; i >= 0; i--) {
            String id = created.get(i);
            try {
                stub().deletePage(DeletePageRequest.newBuilder().setId(id).build());
                stub().deletePage(DeletePageRequest.newBuilder().setId(id).setPurge(true).build());
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                    leftBehind.add("page " + id + ": " + describe(e));
                }
            }
        }
        Set<String> seen = new LinkedHashSet<>();
        rows.forEach(row -> seen.add(row.rpc()));
        for (String rpc : RPCS) {
            if (!seen.contains(rpc)) {
                rows.add(parent == null ? new Row(rpc, rpc, "SKIPPED", "the parent page could not be created")
                        : new Row(rpc, rpc, "FAIL", "not exercised by this test"));
            }
        }
    }

    // ======================================================================
    // STEPS, ROWS, CALLS
    // ======================================================================

    private void step(String rpc, String label, Step body) {
        try {
            rows.add(new Row(rpc, label, "PASS", scrub(body.run())));
        } catch (StatusRuntimeException e) {
            rows.add(new Row(rpc, label, "FAIL", describe(e)));
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            rows.add(new Row(rpc, label, "FAIL", scrub(t.getClass().getSimpleName() + ": " + t.getMessage())));
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private String describe(StatusRuntimeException e) {
        String description = String.valueOf(e.getStatus().getDescription()).replace('\n', ' ');
        return scrub(e.getStatus().getCode() + ": "
                + (description.length() > 300 ? description.substring(0, 300) + "..." : description));
    }

    private String scrub(String text) {
        String out = text == null ? "" : text;
        for (String secret : backend.secrets()) {
            out = out.replace(secret, "***");
        }
        return out;
    }

    /** The run as a Markdown report, one table row per step. */
    String report(String host) {
        StringBuilder md = new StringBuilder("# Confluence live ACL run\n\n");
        md.append("- Site: ").append(host).append(", space ").append(backend.spaceKey()).append('\n');
        Map<String, Integer> outcomes = new TreeMap<>();
        rows.forEach(row -> outcomes.merge(row.outcome(), 1, Integer::sum));
        md.append("- Rows: ").append(rows.size()).append(", outcomes ").append(outcomes).append("\n\n");
        md.append("| rpc | step | outcome | detail |\n|---|---|---|---|\n");
        rows.forEach(row -> md.append("| ").append(row.rpc()).append(" | ").append(row.label()).append(" | ")
                .append(row.outcome()).append(" | ").append(row.detail().replace("|", "\\|")).append(" |\n"));
        md.append("\n## Left behind\n\n").append(leftBehind.isEmpty() ? "Nothing.\n"
                : String.join("\n", leftBehind) + "\n");
        return md.toString();
    }

    private ContentPermissionPrincipal self() {
        return ContentPermissionPrincipal.newBuilder()
                .setPrincipalType(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_USER)
                .setPrincipalId(parent.getAuthorId()).build();
    }

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub() {
        return ConfluenceServiceGrpc.newBlockingStub(channel).withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static ContentRef page(String id) {
        return ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId(id).build();
    }

    private static ContentBody storage(String xhtml) {
        return ContentBody.newBuilder().setStorage(StorageBody.newBuilder().setValue(xhtml)).build();
    }
}
