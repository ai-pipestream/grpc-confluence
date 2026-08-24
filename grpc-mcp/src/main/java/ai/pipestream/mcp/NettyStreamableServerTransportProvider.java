package ai.pipestream.mcp;

import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.ChannelFutureListener;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultHttpContent;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpHeaderNames;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpHeaderValues;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpMethod;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpUtil;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpVersion;
import io.grpc.netty.shaded.io.netty.handler.codec.http.LastHttpContent;
import io.grpc.netty.shaded.io.netty.util.AttributeKey;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.HttpHeaders;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStreamableServerSession;
import io.modelcontextprotocol.spec.McpStreamableServerTransport;
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Streamable HTTP MCP on the same shaded Netty that hosts gRPC. No Jetty.
 */
final class NettyStreamableServerTransportProvider implements McpStreamableServerTransportProvider {

    static final AttributeKey<Runnable> LISTENING_CLOSE = AttributeKey.valueOf("mcpListeningClose");

    private static final String ACCEPT = "Accept";
    private static final String APPLICATION_JSON = "application/json";
    private static final String TEXT_EVENT_STREAM = "text/event-stream";
    private static final String MESSAGE_EVENT_TYPE = "message";
    private static final int REQUEST_MAX_BYTES = 16 * 1024 * 1024;

    private final String endpoint;
    private final McpJsonMapper jsonMapper = McpJsonDefaults.getMapper();
    private final ConcurrentHashMap<String, McpStreamableServerSession> sessions = new ConcurrentHashMap<>();
    private volatile McpStreamableServerSession.Factory sessionFactory;
    private volatile boolean closing;

    NettyStreamableServerTransportProvider(String endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    }

    @Override
    public void setSessionFactory(McpStreamableServerSession.Factory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public Mono<Void> notifyClients(String method, Object params) {
        return Mono.fromRunnable(() -> sessions.values().forEach(session -> {
            try {
                session.sendNotification(method, params).block();
            } catch (RuntimeException ignored) {
                // one failed client must not stop the others
            }
        }));
    }

    @Override
    public Mono<Void> closeGracefully() {
        return Mono.fromRunnable(() -> {
            closing = true;
            sessions.values().forEach(session -> {
                try {
                    session.closeGracefully().block();
                } catch (RuntimeException ignored) {
                    // best-effort
                }
            });
            sessions.clear();
        });
    }

    void serve(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (!pathMatches(request.uri())) {
            writeEmpty(ctx, request, HttpResponseStatus.NOT_FOUND);
            return;
        }
        if (closing) {
            writeEmpty(ctx, request, HttpResponseStatus.SERVICE_UNAVAILABLE);
            return;
        }
        HttpMethod method = request.method();
        if (HttpMethod.POST.equals(method)) {
            doPost(ctx, request);
        } else if (HttpMethod.GET.equals(method)) {
            doGet(ctx, request);
        } else if (HttpMethod.DELETE.equals(method)) {
            doDelete(ctx, request);
        } else {
            writeEmpty(ctx, request, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void doPost(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (request.content().readableBytes() > REQUEST_MAX_BYTES) {
            writeEmpty(ctx, request, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
            return;
        }
        List<String> errors = new ArrayList<>();
        String accept = header(request, ACCEPT);
        if (accept == null || !accept.contains(TEXT_EVENT_STREAM)) {
            errors.add("text/event-stream required in Accept header");
        }
        if (accept == null || !accept.contains(APPLICATION_JSON)) {
            errors.add("application/json required in Accept header");
        }
        String body = request.content().toString(StandardCharsets.UTF_8);
        McpSchema.JSONRPCMessage message;
        try {
            message = McpSchema.deserializeJsonRpcMessage(jsonMapper, body);
        } catch (Exception e) {
            writeError(ctx, request, HttpResponseStatus.BAD_REQUEST,
                    McpSchema.ErrorCodes.INVALID_REQUEST, "Invalid message format: " + e.getMessage());
            return;
        }
        if (message instanceof McpSchema.JSONRPCRequest jsonrpcRequest
                && McpSchema.METHOD_INITIALIZE.equals(jsonrpcRequest.method())) {
            if (!errors.isEmpty()) {
                writeError(ctx, request, HttpResponseStatus.BAD_REQUEST,
                        McpSchema.ErrorCodes.METHOD_NOT_FOUND, String.join("; ", errors));
                return;
            }
            try {
                McpSchema.InitializeRequest initializeRequest = jsonMapper.convertValue(
                        jsonrpcRequest.params(), new TypeRef<>() {
                        });
                McpStreamableServerSession.McpStreamableServerSessionInit init =
                        sessionFactory.startSession(initializeRequest);
                sessions.put(init.session().getId(), init.session());
                McpSchema.InitializeResult initResult = init.initResult().block();
                String json = jsonMapper.writeValueAsString(
                        McpSchema.JSONRPCResponse.result(jsonrpcRequest.id(), initResult));
                writeJson(ctx, request, HttpResponseStatus.OK, json, init.session().getId());
            } catch (Exception e) {
                writeError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        McpSchema.ErrorCodes.INTERNAL_ERROR,
                        "Failed to initialize session: " + e.getMessage());
            }
            return;
        }
        String sessionId = header(request, HttpHeaders.MCP_SESSION_ID);
        if (sessionId == null || sessionId.isBlank()) {
            errors.add("Session ID required in mcp-session-id header");
        }
        if (!errors.isEmpty()) {
            writeError(ctx, request, HttpResponseStatus.BAD_REQUEST,
                    McpSchema.ErrorCodes.METHOD_NOT_FOUND, String.join("; ", errors));
            return;
        }
        McpStreamableServerSession session = sessions.get(sessionId);
        if (session == null) {
            writeError(ctx, request, HttpResponseStatus.NOT_FOUND,
                    McpSchema.ErrorCodes.INTERNAL_ERROR, "Session not found: " + sessionId);
            return;
        }
        McpTransportContext transportContext = McpTransportContext.EMPTY;
        try {
            if (message instanceof McpSchema.JSONRPCResponse jsonrpcResponse) {
                session.accept(jsonrpcResponse)
                        .contextWrite(ctxReactor -> ctxReactor.put(McpTransportContext.KEY, transportContext))
                        .block();
                writeEmpty(ctx, request, HttpResponseStatus.ACCEPTED);
            } else if (message instanceof McpSchema.JSONRPCNotification jsonrpcNotification) {
                session.accept(jsonrpcNotification)
                        .contextWrite(ctxReactor -> ctxReactor.put(McpTransportContext.KEY, transportContext))
                        .block();
                writeEmpty(ctx, request, HttpResponseStatus.ACCEPTED);
            } else if (message instanceof McpSchema.JSONRPCRequest jsonrpcRequest) {
                SseTransport sse = startSse(ctx, sessionId);
                session.responseStream(jsonrpcRequest, sse)
                        .contextWrite(ctxReactor -> ctxReactor.put(McpTransportContext.KEY, transportContext))
                        .block();
            } else {
                writeError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        McpSchema.ErrorCodes.INVALID_REQUEST, "Unknown message type");
            }
        } catch (Exception e) {
            writeError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    McpSchema.ErrorCodes.INTERNAL_ERROR, "Error processing message: " + e.getMessage());
        }
    }

    private void doGet(ChannelHandlerContext ctx, FullHttpRequest request) {
        String accept = header(request, ACCEPT);
        String sessionId = header(request, HttpHeaders.MCP_SESSION_ID);
        if (accept == null || !accept.contains(TEXT_EVENT_STREAM) || sessionId == null
                || sessionId.isBlank()) {
            writeError(ctx, request, HttpResponseStatus.BAD_REQUEST,
                    McpSchema.ErrorCodes.METHOD_NOT_FOUND,
                    "text/event-stream and mcp-session-id are required");
            return;
        }
        McpStreamableServerSession session = sessions.get(sessionId);
        if (session == null) {
            writeEmpty(ctx, request, HttpResponseStatus.NOT_FOUND);
            return;
        }
        SseTransport sse = startSse(ctx, sessionId);
        String lastEvent = header(request, HttpHeaders.LAST_EVENT_ID);
        if (lastEvent != null) {
            try {
                session.replay(lastEvent)
                        .toIterable()
                        .forEach(message -> sse.sendMessage(message).block());
            } finally {
                sse.close();
            }
            return;
        }
        McpStreamableServerSession.McpStreamableServerSessionStream listening =
                session.listeningStream(sse);
        ctx.channel().attr(LISTENING_CLOSE).set(listening::close);
    }

    private void doDelete(ChannelHandlerContext ctx, FullHttpRequest request) {
        String sessionId = header(request, HttpHeaders.MCP_SESSION_ID);
        if (sessionId == null || sessionId.isBlank()) {
            writeError(ctx, request, HttpResponseStatus.BAD_REQUEST,
                    McpSchema.ErrorCodes.METHOD_NOT_FOUND,
                    "Session ID required in mcp-session-id header");
            return;
        }
        McpStreamableServerSession session = sessions.get(sessionId);
        if (session == null) {
            writeEmpty(ctx, request, HttpResponseStatus.NOT_FOUND);
            return;
        }
        try {
            session.delete().block();
            sessions.remove(sessionId);
            writeEmpty(ctx, request, HttpResponseStatus.OK);
        } catch (Exception e) {
            writeError(ctx, request, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    McpSchema.ErrorCodes.INTERNAL_ERROR, e.getMessage());
        }
    }

    private boolean pathMatches(String uri) {
        String path = uri;
        int query = uri.indexOf('?');
        if (query >= 0) {
            path = uri.substring(0, query);
        }
        return path.equals(endpoint) || path.equals(endpoint + "/");
    }

    private static String header(FullHttpRequest request, String name) {
        return request.headers().get(name);
    }

    private SseTransport startSse(ChannelHandlerContext ctx, String sessionId) {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        runOnEventLoop(ctx, () -> ctx.writeAndFlush(response));
        return new SseTransport(ctx, sessionId);
    }

    private void writeJson(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status,
            String json, String sessionId) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer(json, StandardCharsets.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        if (sessionId != null) {
            response.headers().set(HttpHeaders.MCP_SESSION_ID, sessionId);
        }
        HttpUtil.setKeepAlive(response, HttpUtil.isKeepAlive(request));
        flush(ctx, request, response);
    }

    private void writeError(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status,
            int code, String message) {
        try {
            String json = jsonMapper.writeValueAsString(McpError.builder(code).message(message).build());
            writeJson(ctx, request, status, json, null);
        } catch (Exception e) {
            writeEmpty(ctx, request, status);
        }
    }

    private void writeEmpty(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        HttpUtil.setKeepAlive(response, HttpUtil.isKeepAlive(request));
        flush(ctx, request, response);
    }

    private static void flush(ChannelHandlerContext ctx, FullHttpRequest request,
            DefaultFullHttpResponse response) {
        runOnEventLoop(ctx, () -> {
            var future = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(request)) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        });
    }

    private static void runOnEventLoop(ChannelHandlerContext ctx, Runnable action) {
        if (ctx.channel().eventLoop().inEventLoop()) {
            action.run();
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        ctx.channel().eventLoop().execute(() -> {
            try {
                action.run();
            } finally {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final class SseTransport implements McpStreamableServerTransport {

        private final ChannelHandlerContext ctx;
        private final String sessionId;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile boolean closed;

        private SseTransport(ChannelHandlerContext ctx, String sessionId) {
            this.ctx = ctx;
            this.sessionId = sessionId;
        }

        @Override
        public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
            return sendMessage(message, null);
        }

        @Override
        public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message, String messageId) {
            return Mono.fromRunnable(() -> {
                lock.lock();
                try {
                    if (closed) {
                        return;
                    }
                    String json = jsonMapper.writeValueAsString(message);
                    String id = messageId != null ? messageId : sessionId;
                    String frame = "id: " + id + "\nevent: " + MESSAGE_EVENT_TYPE + "\ndata: " + json + "\n\n";
                    runOnEventLoop(ctx, () -> ctx.writeAndFlush(new DefaultHttpContent(
                            Unpooled.copiedBuffer(frame, StandardCharsets.UTF_8))));
                } catch (Exception e) {
                    sessions.remove(sessionId);
                    close();
                } finally {
                    lock.unlock();
                }
            });
        }

        @Override
        public <T> T unmarshalFrom(Object data, TypeRef<T> typeRef) {
            return jsonMapper.convertValue(data, typeRef);
        }

        @Override
        public Mono<Void> closeGracefully() {
            return Mono.fromRunnable(this::close);
        }

        @Override
        public void close() {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                closed = true;
                ctx.channel().attr(LISTENING_CLOSE).set(null);
                runOnEventLoop(ctx, () -> ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT));
            } finally {
                lock.unlock();
            }
        }
    }
}
