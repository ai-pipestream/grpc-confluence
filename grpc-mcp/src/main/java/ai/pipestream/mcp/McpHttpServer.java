package ai.pipestream.mcp;

import io.grpc.netty.shaded.io.netty.bootstrap.ServerBootstrap;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * Streamable HTTP MCP at {@code /mcp}, hosted on the same shaded Netty used
 * for gRPC. Request handling runs on virtual threads. Protocol handlers are
 * the official MCP Java SDK 2.0 {@link McpServer#sync} API.
 */
public final class McpHttpServer implements AutoCloseable {

    /** Streamable HTTP path served by Netty. */
    public static final String ENDPOINT = "/mcp";

    private final Channel channel;
    private final NioEventLoopGroup boss;
    private final NioEventLoopGroup workers;
    private final McpSyncServer mcp;
    private final int port;

    private McpHttpServer(Channel channel, NioEventLoopGroup boss, NioEventLoopGroup workers,
            McpSyncServer mcp, int port) {
        this.channel = channel;
        this.boss = boss;
        this.workers = workers;
        this.mcp = mcp;
        this.port = port;
    }

    /**
     * Returns the port Netty actually bound.
     *
     * @return the local listen port, which may differ from the requested port when that was {@code 0}
     */
    public int port() {
        return port;
    }

    /**
     * Starts Netty HTTP on {@code 0.0.0.0:port} and registers {@code tools}
     * under {@link #ENDPOINT} using the MCP Java SDK {@code sync} API.
     *
     * @param port listen port; {@code 0} selects an ephemeral port
     * @param name MCP {@code serverInfo} name
     * @param tools MCP tool specifications to expose
     * @return a running server whose {@link #port()} is the bound port
     * @throws Exception if Netty fails to bind
     */
    public static McpHttpServer start(int port, String name,
            List<McpServerFeatures.SyncToolSpecification> tools) throws Exception {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(tools, "tools");
        NettyStreamableServerTransportProvider transport =
                new NettyStreamableServerTransportProvider(ENDPOINT);
        var builder = McpServer.sync(transport)
                .serverInfo(name, "0.1.0")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build());
        for (McpServerFeatures.SyncToolSpecification tool : tools) {
            builder.toolCall(tool.tool(), tool.callHandler());
        }
        McpSyncServer mcp = builder.build();

        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup workers = new NioEventLoopGroup(2);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(16 * 1024 * 1024));
                            ch.pipeline().addLast(new McpChannelHandler(transport));
                        }
                    });
            Channel channel = bootstrap.bind(new InetSocketAddress("0.0.0.0", port)).sync()
                    .channel();
            int bound = ((InetSocketAddress) channel.localAddress()).getPort();
            return new McpHttpServer(channel, boss, workers, mcp, bound);
        } catch (Exception e) {
            workers.shutdownGracefully();
            boss.shutdownGracefully();
            mcp.close();
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            mcp.close();
        } catch (Exception ignored) {
        }
        try {
            channel.close().syncUninterruptibly();
        } catch (RuntimeException ignored) {
        }
        workers.shutdownGracefully();
        boss.shutdownGracefully();
    }

    private static final class McpChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final NettyStreamableServerTransportProvider transport;

        private McpChannelHandler(NettyStreamableServerTransportProvider transport) {
            this.transport = transport;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            FullHttpRequest retained = request.retain();
            Thread.startVirtualThread(() -> {
                try {
                    transport.serve(ctx, retained);
                } catch (RuntimeException e) {
                    ctx.close();
                } finally {
                    retained.release();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            Runnable close = ctx.channel().attr(NettyStreamableServerTransportProvider.LISTENING_CLOSE)
                    .getAndSet(null);
            if (close != null) {
                close.run();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
